"""Mode 3: Incremental search by date range with pagination."""

from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery

from config import (
    DEFAULT_BATCH_SIZE,
    GCP_PROJECT_ID,
    MAX_SEARCH_RESULTS,
    PROVIDER_EVENT_TABLE,
    RESULTS_PER_PAGE,
    TiteliveCategory,
)
from src.api.auth import TokenManager
from src.api.client import TiteliveClient
from src.utils.api_transform import extract_gencods_from_search_response
from src.utils.batching import calculate_total_pages
from src.utils.bigquery import (
    count_table_rows,
    deduplicate_table_by_ean,
    get_destination_table_schema,
    get_images_status_breakdown,
    get_last_sync_date,
    get_sample_eans_by_status,
    get_status_breakdown,
    insert_dataframe,
)
from src.utils.ean_processing import process_eans_batch
from src.utils.logging import DailyStats, RunStats, get_logger

logger = get_logger(__name__)


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
    token_manager = TokenManager(project_id)
    api_client = TiteliveClient(token_manager)

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
