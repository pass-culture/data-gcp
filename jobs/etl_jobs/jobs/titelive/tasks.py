"""Titelive ETL tasks: init, incremental, and image download modes."""

import json
import os
from datetime import datetime, timedelta

import pandas as pd
from factories.titelive import TiteliveFactory
from google.cloud import bigquery, storage
from jobs.titelive.batching import calculate_total_pages
from jobs.titelive.config import (
    DE_DATALAKE_BUCKET_NAME,
    DEFAULT_BATCH_SIZE,
    DEFAULT_TARGET_TABLE,
    GCP_PROJECT_ID,
    IMAGE_DOWNLOAD_GCS_PREFIX,
    IMAGE_DOWNLOAD_MAX_WORKERS,
    IMAGE_DOWNLOAD_POOL_CONNECTIONS,
    IMAGE_DOWNLOAD_POOL_MAXSIZE,
    IMAGE_DOWNLOAD_SUB_BATCH_SIZE,
    IMAGE_DOWNLOAD_TIMEOUT,
    MAIN_BATCH_SIZE,
    MAX_SEARCH_RESULTS,
    PROVIDER_EVENT_TABLE,
    RESULTS_PER_PAGE,
    TiteliveCategory,
)
from jobs.titelive.image_utils import (
    _get_session,
    batch_download_and_upload,
    calculate_url_uuid,
)
from jobs.titelive.load import (
    count_failed_eans,
    count_failed_image_downloads,
    count_pending_image_downloads,
    count_table_rows,
    create_destination_table,
    deduplicate_table_by_ean,
    delete_failed_eans,
    fetch_batch_eans,
    fetch_batch_for_image_download,
    fetch_failed_eans,
    get_destination_table_schema,
    get_eans_not_in_product_table,
    get_images_status_breakdown,
    get_last_batch_number,
    get_last_sync_date,
    get_sample_eans_by_images_status,
    get_sample_eans_by_status,
    get_status_breakdown,
    insert_dataframe,
    update_image_download_results,
)
from jobs.titelive.logging_utils import DailyStats, RunStats, get_logger
from jobs.titelive.processing import process_eans_batch
from jobs.titelive.transform import extract_gencods_from_search_response

logger = get_logger(__name__)


def run_init(
    source_table: str,
    destination_table: str,
    main_batch_size: int = MAIN_BATCH_SIZE,
    sub_batch_size: int = DEFAULT_BATCH_SIZE,
    project_id: str = GCP_PROJECT_ID,
    resume: bool = False,
    reprocess_failed: bool = False,
) -> None:
    """
    Execute Mode 1: BigQuery batch processing with batch_number tracking.

    New architecture using single destination table with batch tracking:
    1. Query destination_table ONCE to get last batch_number
    2. For each batch N:
       - Fetch 20k EANs from source_table using OFFSET pagination
       - Process in sub-batches of 250 EANs (API limit)
       - Handle 3 statuses: processed | deleted_in_titelive | fail
       - Write all results with batch_number = N
    3. Increment batch_number in memory (no re-query)

    This eliminates BigQuery buffer issues by querying destination_table only once.

    Optional: Reprocess failed EANs
    - If reprocess_failed=True, fetches EANs with status='failed' from destination_table
    - Deletes failed records, processes via API, inserts with new batch_number
    - Ignores source_table and skip parameters in this mode

    Args:
        source_table: Full table ID for source (project.dataset.table)
        destination_table: Full table ID for destination (project.dataset.table)
        main_batch_size: Number of EANs per batch (default 20,000)
        sub_batch_size: Number of EANs per API call (default 250)
        project_id: GCP project ID
        resume: If True, skip table creation and resume from last batch_number
        reprocess_failed: If True, reprocess EANs with status='failed'

    Raises:
        Exception: If any step fails
    """
    logger.info("Starting Mode 1: BigQuery batch processing (batch_number tracking)")
    logger.info(f"Source: {source_table}, Destination: {destination_table}")
    logger.info(f"Main batch size: {main_batch_size}, Sub-batch size: {sub_batch_size}")

    if reprocess_failed:
        logger.info("Reprocess failed mode: Will fetch and reprocess failed EANs")

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    use_burst_recovery = (
        os.getenv("TITELIVE_USE_BURST_RECOVERY", "false").lower() == "true"
    )
    api_client = TiteliveFactory.create_connector(
        project_id=project_id,
        use_burst_recovery=use_burst_recovery,
        use_enhanced_retry=True,
        use_circuit_breaker=True,
    )

    if not resume and not reprocess_failed:
        # Create destination table if not resuming or reprocessing
        logger.info("Step 1: Creating destination table")
        create_destination_table(bq_client, destination_table, drop_if_exists=True)

    # Step 2: Get last batch number (ONCE - critical for avoiding buffer issues)
    logger.info("Step 2: Getting last batch number from destination table")
    last_batch_number = get_last_batch_number(bq_client, destination_table)
    current_batch = last_batch_number + 1

    logger.info(f"Starting from batch {current_batch}")

    # Step 3: If reprocessing, count total failed EANs
    if reprocess_failed:
        total_failed = count_failed_eans(bq_client, destination_table)
        logger.info(f"Total failed EANs to reprocess: {total_failed}")

        if total_failed == 0:
            logger.info("No failed EANs to reprocess. Exiting.")
            return

    # Step 4: Process batches
    total_processed = 0

    while True:
        # Fetch EANs for this batch
        if reprocess_failed:
            # Fetch failed EANs from destination table (returns tuples)
            batch_ean_pairs = fetch_failed_eans(
                bq_client, destination_table, main_batch_size
            )

            if not batch_ean_pairs:
                logger.info("No more failed EANs to reprocess. Exiting.")
                break
        else:
            # Fetch EANs from source table using OFFSET pagination (returns tuples)
            batch_ean_pairs = fetch_batch_eans(
                bq_client,
                source_table,
                current_batch,
                main_batch_size,
            )

            if not batch_ean_pairs:
                logger.info("No more EANs to process. Exiting.")
                break

        logger.info(
            f"Batch {current_batch}: Processing {len(batch_ean_pairs)} EANs "
            f"in sub-batches of {sub_batch_size}"
        )

        # Process EANs in sub-batches of 250
        results = process_eans_batch(api_client, batch_ean_pairs, sub_batch_size)

        # Count statuses
        status_counts = {}
        for result in results:
            status = result["status"]
            status_counts[status] = status_counts.get(status, 0) + 1

        logger.info(
            f"Batch {current_batch} processed: "
            f"{status_counts.get('processed', 0)} processed, "
            f"{status_counts.get('deleted_in_titelive', 0)} deleted, "
            f"{status_counts.get('failed', 0)} failed"
        )

        # Add processed_at and batch_number to all results
        current_time = datetime.now()
        for result in results:
            result["processed_at"] = current_time
            result["batch_number"] = current_batch
            result["images_download_status"] = None
            result["images_download_processed_at"] = None
            result["recto_image_uuid"] = None
            result["verso_image_uuid"] = None

        # Delete old failed records before inserting new results (if reprocessing)
        if reprocess_failed:
            batch_eans_only = [ean for ean, _ in batch_ean_pairs]
            logger.info(
                f"Batch {current_batch}: Deleting {len(batch_eans_only)} "
                "old failed records"
            )
            delete_failed_eans(bq_client, destination_table, batch_eans_only)

        # Write results to destination table
        logger.info(f"Batch {current_batch}: Writing {len(results)} rows to BigQuery")
        df = pd.DataFrame(results)
        schema = get_destination_table_schema()
        insert_dataframe(
            bq_client,
            destination_table,
            df,
            mode="append",
            schema=schema,
        )

        total_processed += len(batch_ean_pairs)
        logger.info(
            f"Batch {current_batch} complete. Total processed: {total_processed}"
        )

        # Increment batch number in memory (NO re-query - critical!)
        current_batch += 1

    # Step 5: Deduplicate the destination table
    logger.info("Step 5: Deduplicating destination table by EAN")
    deduplicate_table_by_ean(bq_client, destination_table)

    logger.info(
        f"Mode 1 complete. Processed {total_processed} EANs total "
        f"in {current_batch - last_batch_number - 1} batches (after deduplication)."
    )


def run_incremental(
    target_table: str,
    results_per_page: int = RESULTS_PER_PAGE,
    project_id: str = GCP_PROJECT_ID,
    provider_event_table: str = PROVIDER_EVENT_TABLE,
    window_days: int = 7,
) -> None:
    """
    Execute incremental date range search using a sliding window approach.

    This function implements a sliding window strategy to handle late-arriving data
    from the Titelive API. On day D, it processes:
    - The target sync date (last_sync_date from provider event table)
    - A lookback window of the previous X days (default: 7 days)

    This assumes that API results stabilize after X days, ensuring complete data
    capture.

    Workflow:
    1. Query last sync date for each base from provider event table
    2. Generate sliding window: [last_sync_date - window_days, last_sync_date]
    3. Truncate table before processing
    4. For each date and each base ("paper" and "music"):
       - Initial search request to check total results
       - Verify nbresults < 20000 (API limit)
       - Calculate total pages and collect all gencods
       - Fetch detailed data via /ean endpoint
       - Transform and append results to target table
    5. Deduplicate by EAN

    Args:
        target_table: Full table ID for target (project.dataset.table)
        results_per_page: Number of results per page (default: 120)
        project_id: GCP project ID
        provider_event_table: Full table ID for provider event table
        window_days: Number of days to look back for catch-up (default: 7)

    Raises:
        ValueError: If total results exceed API limit for any base/date
        Exception: If any step fails
    """
    # Initialize run statistics
    run_stats = RunStats()

    logger.info("=" * 70)
    logger.info("STARTING INCREMENTAL SYNC")
    logger.info("=" * 70)
    logger.info(f"Target table: {target_table}")
    logger.info(f"Window size: {window_days} days")

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    use_burst_recovery = (
        os.getenv("TITELIVE_USE_BURST_RECOVERY", "false").lower() == "true"
    )
    api_client = TiteliveFactory.create_connector(
        project_id=project_id,
        use_burst_recovery=use_burst_recovery,
        use_enhanced_retry=True,
        use_circuit_breaker=True,
    )

    # Define bases to process
    bases = [base.value for base in TiteliveCategory]

    # Step 1: Get last sync dates for each base
    logger.info("-" * 70)
    logger.info("STEP 1: Querying last sync dates from provider event table")
    today = datetime.now().date()

    for base in bases:
        last_sync_date = get_last_sync_date(bq_client, provider_event_table, base)
        if last_sync_date:
            sync_date = datetime.strptime(last_sync_date, "%Y-%m-%d").date()
            run_stats.last_sync_dates[base] = sync_date

            # Check staleness (warn if data is older than window_days)
            days_stale = (today - sync_date).days
            if days_stale > run_stats.staleness_days:
                run_stats.staleness_days = days_stale

            if days_stale > window_days:
                logger.warning(
                    f"⚠️  STALE DATA: {base} last synced {days_stale} days ago "
                    f"({last_sync_date}). Backend sync may be broken!"
                )
                run_stats.anomalies.append(
                    f"{base} data is {days_stale} days stale "
                    f"(last sync: {last_sync_date})"
                )
        else:
            logger.warning(f"No previous sync found for {base}, skipping")
            run_stats.last_sync_dates[base] = None

    logger.info(f"Last sync dates: {run_stats.last_sync_dates}")
    logger.info(f"Today: {today}, Staleness: {run_stats.staleness_days} days")

    # Step 2: Generate list of dates to process using sliding window
    logger.info("-" * 70)
    logger.info("STEP 2: Generating date window")

    all_dates = set()
    for base in bases:
        sync_date = run_stats.last_sync_dates.get(base)
        if sync_date is not None:
            end_date = sync_date
            start_date = end_date - timedelta(days=window_days)

            current_date = start_date
            while current_date <= end_date:
                all_dates.add(current_date)
                current_date += timedelta(days=1)

    if not all_dates:
        logger.info("No dates to process. All bases are up to date.")
        run_stats.log_summary(logger)
        return

    dates_to_process = sorted(all_dates)
    run_stats.window_start = dates_to_process[0]
    run_stats.window_end = dates_to_process[-1]
    run_stats.days_processed = len(dates_to_process)

    logger.info(
        f"Window: {run_stats.window_start} to {run_stats.window_end} "
        f"({run_stats.days_processed} days)"
    )

    # Step 3: Check current state and truncate table
    logger.info("-" * 70)
    logger.info("CHECKPOINT 1: INPUT STATE (before truncate)")

    # Log current table state before truncate
    try:
        pre_truncate_count = count_table_rows(bq_client, target_table)
        run_stats.pre_truncate_count = pre_truncate_count
        logger.info(
            f"Current table has {pre_truncate_count:,} rows (will be truncated)"
        )

        # Get status breakdown before truncate
        status_breakdown = get_status_breakdown(bq_client, target_table)
        logger.info("Status breakdown before truncate:")
        for status, count in status_breakdown.items():
            logger.info(f"  - {status}: {count:,}")

        # Get images status breakdown
        images_breakdown = get_images_status_breakdown(bq_client, target_table)
        logger.info("Images download status before truncate:")
        for status, count in images_breakdown.items():
            logger.info(f"  - {status}: {count:,}")

    except Exception as e:
        logger.info(f"Table doesn't exist yet or is empty: {e}")
        run_stats.pre_truncate_count = 0

    # Truncate table
    logger.info("-" * 70)
    logger.info("STEP 3: Truncating target table")
    truncate_query = f"TRUNCATE TABLE `{target_table}`"
    try:
        truncate_job = bq_client.query(truncate_query)
        truncate_job.result()
        logger.info("Target table truncated successfully")
    except Exception as e:
        logger.warning(f"Could not truncate table (might not exist yet): {e}")

    # Step 4: Process each date
    logger.info("-" * 70)
    logger.info("STEP 4: Processing dates")

    for process_date in dates_to_process:
        date_str = process_date.strftime("%Y-%m-%d")

        # Calculate API date range
        next_day = process_date + timedelta(days=1)
        min_date_formatted = _format_date_for_api(date_str)
        max_date_formatted = _format_date_for_api(next_day.strftime("%Y-%m-%d"))

        # Process both bases for this date
        for base in bases:
            sync_date = run_stats.last_sync_dates.get(base)
            if sync_date is None:
                continue

            # Skip if outside this base's window
            base_start_date = sync_date - timedelta(days=window_days)
            if not (base_start_date <= process_date <= sync_date):
                continue

            # Initialize daily stats
            daily_stats = DailyStats(date=date_str, base=base)

            # Get total results from API
            initial_response = api_client.search_by_date(
                base=base,
                min_date=min_date_formatted,
                max_date=max_date_formatted,
                page=1,
            )

            total_results = initial_response.get("nbreponses", 0)
            daily_stats.api_results_count = total_results

            # Detect anomalies
            if total_results == 0:
                run_stats.anomalies.append(f"Zero results for {base} on {date_str}")
                run_stats.add_daily_stats(daily_stats)
                continue

            # Validate against API limit
            if total_results >= MAX_SEARCH_RESULTS:
                msg = (
                    f"Total results for {base} on {date_str} ({total_results}) "
                    f"exceeds API limit ({MAX_SEARCH_RESULTS})."
                )
                raise ValueError(msg)

            # Phase 1: Collect all gencods from search pages
            total_pages = calculate_total_pages(total_results, results_per_page)
            all_gencods = []
            total_filtered = 0
            filtered_samples = []

            for page in range(1, total_pages + 1):
                try:
                    response = api_client.search_by_date(
                        base=base,
                        min_date=min_date_formatted,
                        max_date=max_date_formatted,
                        page=page,
                        results_per_page=results_per_page,
                    )

                    results = response.get("result", [])
                    if results:
                        page_gencods, filter_stats = (
                            extract_gencods_from_search_response(
                                response, from_date=min_date_formatted
                            )
                        )
                        all_gencods.extend(page_gencods)

                        # Aggregate filter stats
                        total_filtered += filter_stats["filtered_count"]
                        if len(filtered_samples) < 5:
                            filtered_samples.extend(filter_stats["filtered_samples"])

                except Exception as e:
                    logger.error(
                        f"Error fetching page {page} for {base}/{date_str}: {e}"
                    )
                    continue

            # Log filtered gencods if any
            daily_stats.filtered_by_date = total_filtered
            if total_filtered > 0:
                logger.info(
                    f"    ⚠️ Filtered {total_filtered} gencods "
                    f"(datemodification < {min_date_formatted})"
                )
                if filtered_samples[:5]:
                    logger.info("    Sample filtered EANs:")
                    for gencod, date_mod in filtered_samples[:5]:
                        logger.info(f"      - {gencod} (dateMaj={date_mod})")

            # Phase 2: Fetch detailed data via /ean endpoint
            if all_gencods:
                unique_gencods = list(set(all_gencods))
                daily_stats.gencods_collected = len(unique_gencods)

                # Map base to subcategoryid
                if base == TiteliveCategory.MUSIC:
                    representative_subcategoryid = "SUPPORT_PHYSIQUE_MUSIQUE_CD"
                else:
                    representative_subcategoryid = None

                ean_pairs = [
                    (gencod, representative_subcategoryid) for gencod in unique_gencods
                ]

                # Process EANs (reduced logging in process_eans_batch)
                results = process_eans_batch(
                    api_client, ean_pairs, sub_batch_size=DEFAULT_BATCH_SIZE
                )

                # Count statuses and collect samples
                failed_eans = []
                deleted_eans = []
                for result in results:
                    if result["status"] == "processed":
                        daily_stats.eans_processed += 1
                    elif result["status"] == "deleted_in_titelive":
                        daily_stats.eans_deleted += 1
                        if len(deleted_eans) < 5:
                            deleted_eans.append(result["ean"])
                    elif result["status"] == "failed":
                        daily_stats.eans_failed += 1
                        if len(failed_eans) < 5:
                            failed_eans.append(result["ean"])

                # Log sample failed/deleted EANs
                if deleted_eans:
                    daily_stats.sample_deleted_eans = deleted_eans
                    logger.info(
                        f"    Sample deleted EANs ({daily_stats.eans_deleted} total):"
                    )
                    for ean in deleted_eans:
                        logger.info(f"      - {ean}")

                if failed_eans:
                    daily_stats.sample_failed_eans = failed_eans
                    logger.info(
                        f"    ⚠️ Sample failed EANs ({daily_stats.eans_failed} total):"
                    )
                    for ean in failed_eans:
                        logger.info(f"      - {ean}")

                # Add required fields
                current_time = datetime.now()
                for result in results:
                    result["processed_at"] = current_time
                    result["batch_number"] = 0
                    result["images_download_status"] = None
                    result["images_download_processed_at"] = None
                    result["recto_image_uuid"] = None
                    result["verso_image_uuid"] = None

                # Insert to BigQuery
                combined_df = pd.DataFrame(results)
                schema = get_destination_table_schema()
                insert_dataframe(
                    bq_client,
                    target_table,
                    combined_df,
                    mode="append",
                    schema=schema,
                )

                daily_stats.rows_inserted = len(combined_df)

            run_stats.add_daily_stats(daily_stats)

            # Log progress (single line per base/date)
            logger.info(
                f"  {date_str} {base}: API={daily_stats.api_results_count:,} → "
                f"Gencods={daily_stats.gencods_collected:,} → "
                f"Inserted={daily_stats.rows_inserted:,} "
                f"(failed={daily_stats.eans_failed})"
            )

    # Step 5: Deduplicate and get final count
    logger.info("-" * 70)
    logger.info("STEP 5: Deduplicating and counting final rows")
    deduplicate_table_by_ean(bq_client, target_table)

    # Get actual row count after deduplication
    run_stats.total_rows_after_dedup = count_table_rows(bq_client, target_table)

    # Final verification checkpoint
    logger.info("-" * 70)
    logger.info("CHECKPOINT: FINAL VERIFICATION")
    logger.info("-" * 70)

    # Status breakdown
    final_status_breakdown = get_status_breakdown(bq_client, target_table)
    run_stats.final_status_breakdown = final_status_breakdown
    logger.info("Final status breakdown:")
    for status, count in final_status_breakdown.items():
        pct = (
            (count / run_stats.total_rows_after_dedup * 100)
            if run_stats.total_rows_after_dedup > 0
            else 0
        )
        logger.info(f"  - {status}: {count:,} ({pct:.1f}%)")

    # Sample failed EANs if any
    failed_count = final_status_breakdown.get("failed", 0)
    if failed_count > 0:
        sample_failed = get_sample_eans_by_status(bq_client, target_table, "failed", 5)
        logger.warning(f"⚠️ {failed_count} failed EANs. Sample:")
        for ean in sample_failed:
            logger.warning(f"    - {ean}")

    # Images download status (should all be NULL at this point)
    images_status = get_images_status_breakdown(bq_client, target_table)
    run_stats.final_images_status_breakdown = images_status
    logger.info("Images download status (pre-download_images):")
    for status, count in images_status.items():
        logger.info(f"  - {status}: {count:,}")

    null_images = images_status.get("NULL", 0)
    if null_images == run_stats.total_rows_after_dedup:
        logger.info("  ✓ All EANs pending image download (expected)")
    else:
        logger.warning(
            f"  ⚠️ Unexpected: {run_stats.total_rows_after_dedup - null_images} "
            f"EANs already have images_download_status set"
        )

    # Log comprehensive summary
    run_stats.log_summary(logger)


def _format_date_for_api(date_str: str) -> str:
    """
    Convert date from YYYY-MM-DD to DD/MM/YYYY format.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        Date string in DD/MM/YYYY format

    Raises:
        ValueError: If date format is invalid
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%d/%m/%Y")
    except ValueError as e:
        msg = f"Invalid date format: {date_str}. Expected YYYY-MM-DD."
        raise ValueError(msg) from e


def run_download_images(
    reprocess_failed: bool = False,
) -> None:
    """
    Download images from URLs in BigQuery table and upload to GCS.

    Unified architecture for both normal and reprocess modes:
    1. Iterate through batch_numbers (0, 1, 2, ...)
    2. For each batch_number:
       - Fetch ALL EANs for this batch (up to 20k) with matching status filter
       - Chunk into sub-batches of 1000 EANs
       - Process each sub-batch: threaded download + upload to GCS
       - Accumulate all results in memory
       - Write ALL results to BigQuery once when batch is complete
    3. Move to next batch_number

    Status filters:
    - Normal mode: images_download_status IS NULL (pending)
    - Reprocess mode: images_download_status = 'failed' (retry)

    All configuration loaded from config.py:
    - source_table: DEFAULT_TARGET_TABLE
    - gcs_bucket: DE_DATALAKE_BUCKET_NAME
    - gcs_prefix: IMAGE_DOWNLOAD_GCS_PREFIX
    - max_workers: IMAGE_DOWNLOAD_MAX_WORKERS
    - pool_connections: IMAGE_DOWNLOAD_POOL_CONNECTIONS
    - pool_maxsize: IMAGE_DOWNLOAD_POOL_MAXSIZE
    - timeout: IMAGE_DOWNLOAD_TIMEOUT
    - sub_batch_size: IMAGE_DOWNLOAD_SUB_BATCH_SIZE

    Args:
        reprocess_failed: If True, reprocess failed downloads; if False, process pending
    """
    source_table = DEFAULT_TARGET_TABLE
    gcs_bucket = DE_DATALAKE_BUCKET_NAME
    gcs_prefix = IMAGE_DOWNLOAD_GCS_PREFIX
    max_workers = IMAGE_DOWNLOAD_MAX_WORKERS
    pool_connections = IMAGE_DOWNLOAD_POOL_CONNECTIONS
    pool_maxsize = IMAGE_DOWNLOAD_POOL_MAXSIZE
    timeout = IMAGE_DOWNLOAD_TIMEOUT
    sub_batch_size = IMAGE_DOWNLOAD_SUB_BATCH_SIZE

    mode_label = "reprocess failed" if reprocess_failed else "normal"
    logger.info("=" * 70)
    logger.info("STARTING IMAGE DOWNLOAD")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode_label}")
    logger.info(f"Source table: {source_table}")
    logger.info(f"Target: gs://{gcs_bucket}/{gcs_prefix}")
    logger.info(f"Max workers: {max_workers}, Timeout: {timeout}s")
    logger.info(f"Sub-batch size: {sub_batch_size} EANs")

    # Initialize clients
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    session = _get_session(pool_connections, pool_maxsize, timeout)

    # CHECKPOINT 1: Initial state
    logger.info("-" * 70)
    logger.info("CHECKPOINT 1: INPUT STATE")
    logger.info("-" * 70)

    # Status breakdown
    status_breakdown = get_status_breakdown(bq_client, source_table)
    logger.info("EAN status breakdown:")
    for status, count in status_breakdown.items():
        logger.info(f"  - {status}: {count:,}")

    # Images status breakdown
    images_breakdown = get_images_status_breakdown(bq_client, source_table)
    logger.info("Images download status breakdown:")
    for status, count in images_breakdown.items():
        logger.info(f"  - {status}: {count:,}")

    # Get total count based on mode
    if reprocess_failed:
        total_count = count_failed_image_downloads(bq_client, source_table)
        mode_label = "failed"
    else:
        total_count = count_pending_image_downloads(bq_client, source_table)
        mode_label = "pending"

    logger.info(f"Total {mode_label} image downloads to process: {total_count:,}")

    if total_count == 0:
        logger.info(f"No {mode_label} image downloads. Exiting.")
        return

    # Get last batch number to know when to stop
    last_batch = get_last_batch_number(bq_client, source_table)
    logger.info(f"Last batch number in table: {last_batch}")

    # Start from batch 0 and iterate through all batches
    current_batch = 0
    total_processed = 0
    total_success = 0
    total_failed = 0

    # Process batches
    while current_batch <= last_batch:
        logger.info("-" * 70)
        logger.info(f"CHECKPOINT: BATCH {current_batch}")
        logger.info("-" * 70)

        # Fetch ALL EANs for this batch (up to 20k)
        all_rows = fetch_batch_for_image_download(
            bq_client, source_table, current_batch, reprocess_failed
        )

        if not all_rows:
            logger.info(
                f"No {mode_label} EANs in batch {current_batch}. Moving to next."
            )
            current_batch += 1
            continue

        logger.info(f"Fetched {len(all_rows)} {mode_label} EANs for processing")

        # Log EANs not in product_table (for debugging sync lag issues)
        not_in_product_count, sample_not_in_product = get_eans_not_in_product_table(
            bq_client, source_table, current_batch
        )
        if not_in_product_count > 0:
            logger.info(
                f"  [i] {not_in_product_count:,} EANs not yet in product_table "
                "(will still be processed)"
            )
            if sample_not_in_product:
                logger.info(f"    Sample: {sample_not_in_product}")

        # Check how many have old UUIDs (for change detection)
        eans_with_old_uuid = sum(
            1
            for r in all_rows
            if r.get("old_recto_image_uuid") or r.get("old_verso_image_uuid")
        )
        logger.info(
            f"  {eans_with_old_uuid:,} EANs have existing UUIDs (change detection)"
        )
        logger.info(
            f"  {len(all_rows) - eans_with_old_uuid:,} EANs are new (no old UUIDs)"
        )

        # Accumulate results for entire batch
        batch_results = []

        # Chunk into sub-batches and process
        for i in range(0, len(all_rows), sub_batch_size):
            sub_batch = all_rows[i : i + sub_batch_size]
            sub_batch_num = (i // sub_batch_size) + 1
            total_sub_batches = (len(all_rows) + sub_batch_size - 1) // sub_batch_size

            logger.info(
                f"  Sub-batch {sub_batch_num}/{total_sub_batches}: "
                f"Processing {len(sub_batch)} EANs"
            )

            # Process images for this sub-batch
            sub_batch_results = _process_batch_images(
                sub_batch,
                storage_client,
                session,
                gcs_bucket,
                gcs_prefix,
                max_workers,
                timeout,
            )

            # Accumulate results
            batch_results.extend(sub_batch_results)

            # Count sub-batch results
            sub_success = sum(
                1
                for r in sub_batch_results
                if r["images_download_status"] == "processed"
            )
            sub_failed = sum(
                1 for r in sub_batch_results if r["images_download_status"] == "failed"
            )

            logger.info(
                f"  Sub-batch {sub_batch_num}/{total_sub_batches} complete: "
                f"{sub_success} processed, {sub_failed} failed"
            )

        # Write ALL results for this batch_number to BigQuery once
        logger.info(
            f"Writing {len(batch_results)} results to BigQuery "
            f"for batch {current_batch}"
        )
        update_image_download_results(bq_client, source_table, batch_results)

        # Count batch results
        batch_success = sum(
            1 for r in batch_results if r["images_download_status"] == "processed"
        )
        batch_failed = sum(
            1 for r in batch_results if r["images_download_status"] == "failed"
        )

        logger.info(
            f"Batch {current_batch} complete: "
            f"{len(batch_results)} EANs processed "
            f"({batch_success} success, {batch_failed} failed)"
        )

        total_processed += len(batch_results)
        total_success += batch_success
        total_failed += batch_failed

        logger.info(
            f"Overall progress: {total_processed:,} EANs processed "
            f"({total_success:,} success, {total_failed:,} failed)"
        )

        # Move to next batch
        current_batch += 1

    # CHECKPOINT: FINAL VERIFICATION
    logger.info("-" * 70)
    logger.info("CHECKPOINT: FINAL VERIFICATION")
    logger.info("-" * 70)

    # Final images status breakdown
    final_images_breakdown = get_images_status_breakdown(bq_client, source_table)
    logger.info("Final images_download_status breakdown:")
    for status, count in final_images_breakdown.items():
        pct = count / sum(final_images_breakdown.values()) * 100
        logger.info(f"  - {status}: {count:,} ({pct:.1f}%)")

    # Check for unexpected NULL status
    null_count = final_images_breakdown.get("NULL", 0)
    if null_count > 0:
        logger.warning(f"⚠️ {null_count:,} EANs still have NULL images_download_status!")
        sample_null = get_sample_eans_by_images_status(bq_client, source_table, None, 5)
        logger.warning(f"  Sample NULL EANs: {sample_null}")
    else:
        logger.info("✓ All EANs have been processed (no NULL status remaining)")

    # Check for failed EANs
    failed_img_count = final_images_breakdown.get("failed", 0)
    if failed_img_count > 0:
        sample_failed = get_sample_eans_by_images_status(
            bq_client, source_table, "failed", 5
        )
        logger.warning(f"⚠️ {failed_img_count:,} EANs failed image download")
        logger.warning(f"  Sample failed EANs: {sample_failed}")

    # Summary
    logger.info("=" * 70)
    logger.info("IMAGE DOWNLOAD SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total processed: {total_processed:,}")
    logger.info(f"  - Success: {total_success:,}")
    logger.info(f"  - Failed: {total_failed:,}")
    no_change_total = total_processed - total_success - total_failed
    if no_change_total > 0:
        logger.info(f"  - No change: {no_change_total:,}")
    logger.info("=" * 70)


def _process_batch_images(
    rows: list[dict],
    storage_client: storage.Client,
    session,
    gcs_bucket: str,
    gcs_prefix: str,
    max_workers: int,
    timeout: int,
) -> list[dict]:
    """
    Process images for a batch of EANs using threaded download + upload.

    Extracts recto/verso URLs from json_raw, downloads both images with
    requests, and uploads to GCS with google.cloud.storage.

    Args:
        rows: List of dicts with keys: ean, json_raw
        storage_client: GCS storage client
        session: requests.Session for connection pooling
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS path prefix
        max_workers: Maximum concurrent workers
        timeout: HTTP request timeout in seconds

    Returns:
        List of dicts with keys: ean, images_download_status,
        images_download_processed_at
    """
    # Extract image URLs and prepare download list
    download_tasks = []
    ean_to_images = {}  # Maps EAN to list of (url, gcs_path, image_type)
    ean_to_uuids = {}  # Maps EAN to dict with 'recto' and 'verso' UUIDs
    failed_eans = set()  # Track EANs that had parsing errors
    no_image_eans = set()  # Track EANs with ONLY "no_image" placeholder URLs
    no_image_url_count = 0  # Track total placeholder "no_image" URLs
    no_image_cache = {}  # In-memory cache: no_image URL -> UUID (minimizes GCS calls)

    # Filtering stats
    skipped_recto_count = 0  # image field != 1
    skipped_verso_count = 0  # image_4 field != 1
    unchanged_url_count = 0  # URL hash unchanged
    sample_skipped_recto = []
    sample_skipped_verso = []
    sample_parse_errors = []

    for row in rows:
        ean = row["ean"]
        json_raw = row["json_raw"]
        old_recto_uuid = row.get("old_recto_image_uuid")
        old_verso_uuid = row.get("old_verso_image_uuid")

        # Parse JSON to extract image URLs
        try:
            data = json.loads(json_raw)

            # Handle both article formats:
            # - List format: article = [{}]
            # - Dict format: article = {"1": {...}, "2": {...}}
            article_data = data.get("article")
            if isinstance(article_data, list):
                # List format - take first element
                article = article_data[0] if article_data else {}
            elif isinstance(article_data, dict):
                # Dict format - take first numeric key's value
                numeric_keys = sorted([k for k in article_data if k.isdigit()])
                article = article_data[numeric_keys[0]] if numeric_keys else {}
            else:
                article = {}

            images_url = article.get("imagesUrl", {})

            recto_url = images_url.get("recto")
            verso_url = images_url.get("verso")

            # Validate image availability based on image and image_4 fields
            image_field = article.get("image")
            image_4_field = article.get("image_4")

            if image_field != 1:
                skipped_recto_count += 1
                if len(sample_skipped_recto) < 3:
                    sample_skipped_recto.append((ean, image_field))
                recto_url = None

            if image_4_field != 1:
                skipped_verso_count += 1
                if len(sample_skipped_verso) < 3:
                    sample_skipped_verso.append((ean, image_4_field))
                verso_url = None

            ean_images = []
            ean_has_real_image = False
            ean_has_placeholder = False
            recto_uuid = None
            verso_uuid = None

            # Process recto with hash comparison
            is_placeholder, image_added, recto_uuid = _process_image_url(
                recto_url,
                "recto",
                gcs_bucket,
                gcs_prefix,
                download_tasks,
                ean_images,
                old_recto_uuid,
                storage_client,
                no_image_cache,
            )
            if is_placeholder:
                ean_has_placeholder = True
                no_image_url_count += 1
            if image_added:
                ean_has_real_image = True
            elif recto_url and not is_placeholder:
                unchanged_url_count += 1

            # Process verso with hash comparison
            is_placeholder, image_added, verso_uuid = _process_image_url(
                verso_url,
                "verso",
                gcs_bucket,
                gcs_prefix,
                download_tasks,
                ean_images,
                old_verso_uuid,
                storage_client,
                no_image_cache,
            )
            if is_placeholder:
                ean_has_placeholder = True
                no_image_url_count += 1
            if image_added:
                ean_has_real_image = True
            elif verso_url and not is_placeholder:
                unchanged_url_count += 1

            ean_to_images[ean] = ean_images
            ean_to_uuids[ean] = {"recto": recto_uuid, "verso": verso_uuid}

            # Track EANs with ONLY placeholder images (no real images)
            if ean_has_placeholder and not ean_has_real_image:
                no_image_eans.add(ean)

        except Exception as e:
            if len(sample_parse_errors) < 3:
                sample_parse_errors.append((ean, str(e)))
            ean_to_images[ean] = []
            ean_to_uuids[ean] = {"recto": None, "verso": None}
            failed_eans.add(ean)

    # Log filtering breakdown
    logger.info("  Filtering breakdown:")
    logger.info(f"    - Skipped recto (image!=1): {skipped_recto_count}")
    if sample_skipped_recto:
        logger.info(f"      Sample: {sample_skipped_recto}")
    logger.info(f"    - Skipped verso (image_4!=1): {skipped_verso_count}")
    if sample_skipped_verso:
        logger.info(f"      Sample: {sample_skipped_verso}")
    logger.info(f"    - Placeholder 'no_image' URLs: {no_image_url_count}")
    logger.info(f"    - Unchanged URLs (hash match): {unchanged_url_count}")
    logger.info(f"    - Parse errors: {len(failed_eans)}")
    if sample_parse_errors:
        logger.info(f"      Sample errors: {sample_parse_errors}")
    logger.info(f"  → Real images to download: {len(download_tasks)}")

    # Download all images using threaded approach
    if download_tasks:
        image_urls = [task[0] for task in download_tasks]
        gcs_paths = [task[1] for task in download_tasks]

        logger.info(f"Downloading {len(download_tasks)} images")

        # Run threaded download
        download_results = batch_download_and_upload(
            storage_client=storage_client,
            session=session,
            image_urls=image_urls,
            gcs_paths=gcs_paths,
            max_workers=max_workers,
            timeout=timeout,
        )

        # Map results back to URLs
        url_to_result = {
            url: (success, message) for success, url, message in download_results
        }
    else:
        url_to_result = {}

    # Build final results per EAN
    results = []
    current_time = datetime.now()

    for ean, images in ean_to_images.items():
        # Get UUIDs for this EAN
        uuids = ean_to_uuids.get(ean, {"recto": None, "verso": None})
        recto_uuid = uuids.get("recto")
        verso_uuid = uuids.get("verso")

        # Check if this EAN had a parsing error
        if ean in failed_eans:
            # Parsing failed - mark as failed
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "failed",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )
            continue

        # Check if this EAN has only placeholder images
        if ean in no_image_eans:
            # Only placeholder images - mark as processed without download
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "processed",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )
            continue

        if not images:
            # No images to download (all skipped due to unchanged URLs or no URLs)
            # Mark as 'no_change' - URLs haven't changed
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "no_change",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )
            continue

        # Check if all images succeeded
        all_success = True
        for img in images:
            success, _ = url_to_result.get(img["url"], (False, "Unknown error"))
            if not success:
                all_success = False
                break

        if all_success:
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "processed",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )
        else:
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "failed",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )

    # Log final summary
    processed_count = sum(
        1 for r in results if r["images_download_status"] == "processed"
    )
    failed_count = sum(1 for r in results if r["images_download_status"] == "failed")
    no_change_count = sum(
        1 for r in results if r["images_download_status"] == "no_change"
    )

    logger.info(
        f"Batch complete: {processed_count} EANs processed, "
        f"{no_change_count} no change, "
        f"{failed_count} failed "
        f"({len(no_image_eans)} with placeholders only)"
    )

    return results


def _process_image_url(
    image_url: str | None,
    image_type: str,
    gcs_bucket: str,
    gcs_prefix: str,
    download_tasks: list,
    ean_images: list,
    old_uuid: str | None = None,
    storage_client: storage.Client | None = None,
    no_image_cache: dict[str, str] | None = None,
) -> tuple[bool, bool, str | None]:
    """
    Process a single image URL (recto or verso) and add to download tasks if URL changed

    Handles placeholder "no_image" URLs with caching and GCS existence checks:
    - First encounter: checks cache, then GCS bucket, downloads if needed
    - Subsequent encounters: instant cache hit (zero I/O)

    Uses UUID5 hash-based change detection:
    - Calculates new UUID from URL (deterministic)
    - Compares with old UUID
    - Only downloads if UUID changed

    Args:
        image_url: URL of the image to download (None if not present)
        image_type: Type of image ("recto" or "verso")
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS path prefix
        download_tasks: List to append download task to (modified in place)
        ean_images: List to append image info to (modified in place)
        old_uuid: Previous UUID from product_mediation (None if first time)
        storage_client: GCS client for checking blob existence
        no_image_cache: In-memory cache mapping no_image URL -> UUID

    Returns:
        Tuple of (is_placeholder: bool, image_added: bool, uuid: str | None)
        - is_placeholder: True if a "no_image" URL was found
        - image_added: True if a real image was added to download tasks
        - uuid: UUID5 of the image URL (None if no URL)
    """
    if image_url:
        # Check if placeholder "no_image" URL
        if "no_image" in image_url.lower():
            # Check in-memory cache first (zero I/O)
            if no_image_cache is not None and image_url in no_image_cache:
                cached_uuid = no_image_cache[image_url]
                return (True, False, cached_uuid)

            # First time seeing this no_image URL - calculate UUID
            uuid = calculate_url_uuid(image_url)

            # Check if blob exists in GCS (one call per unique no_image URL)
            if storage_client is not None:
                bucket = storage_client.bucket(gcs_bucket)
                blob_path = f"{gcs_prefix}/{uuid}"
                blob = bucket.blob(blob_path)

                if not blob.exists():
                    # Blob doesn't exist - add to download tasks
                    gcs_path = f"gs://{gcs_bucket}/{blob_path}"
                    download_tasks.append((image_url, gcs_path))

            # Cache the UUID for subsequent EANs
            if no_image_cache is not None:
                no_image_cache[image_url] = uuid

            return (True, False, uuid)  # is_placeholder=True, image_added=False

        # Calculate deterministic UUID5 from URL
        new_uuid = calculate_url_uuid(image_url)

        # Compare with old UUID - only download if changed
        if new_uuid != old_uuid:
            # URL changed - need to download
            gcs_path = f"gs://{gcs_bucket}/{gcs_prefix}/{new_uuid}"

            download_tasks.append((image_url, gcs_path))
            ean_images.append(
                {"url": image_url, "gcs_path": gcs_path, "type": image_type}
            )
            return (
                False,
                True,
                new_uuid,
            )  # is_placeholder=False, image_added=True, uuid=new_uuid
        else:
            # URL unchanged - skip download
            return (
                False,
                False,
                new_uuid,
            )  # is_placeholder=False, image_added=False, uuid=new_uuid

    return (False, False, None)  # No URL present
