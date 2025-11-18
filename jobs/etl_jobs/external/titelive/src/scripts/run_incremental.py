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
    deduplicate_table_by_ean,
    get_destination_table_schema,
    get_last_sync_date,
    insert_dataframe,
)
from src.utils.ean_processing import process_eans_batch
from src.utils.logging import get_logger

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
    logger.info("Starting incremental sync for both bases")
    logger.info(f"Target: {target_table}")

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    token_manager = TokenManager(project_id)
    api_client = TiteliveClient(token_manager)

    # Define bases to process
    bases = [base.value for base in TiteliveCategory]

    # Step 1: Get last sync dates for each base
    logger.info("Step 1: Querying last sync dates from provider event table")
    last_sync_dates = {}
    for base in bases:
        last_sync_date = get_last_sync_date(bq_client, provider_event_table, base)
        if last_sync_date:
            last_sync_dates[base] = datetime.strptime(last_sync_date, "%Y-%m-%d").date()
        else:
            # If no previous sync, we'll skip this base
            logger.warning(f"No previous sync found for {base}, skipping")
            last_sync_dates[base] = None

    # Step 2: Generate list of dates to process using sliding window
    # For each base, we process: [last_sync_date - window_days, last_sync_date]
    # This allows catching late-arriving data within the window

    # Collect all date ranges for all bases
    all_dates = set()
    for base in bases:
        if last_sync_dates[base] is not None:
            end_date = last_sync_dates[base]
            start_date = end_date - timedelta(days=window_days)

            logger.info(
                f"{base}: Processing window from {start_date} to {end_date} "
                f"({window_days + 1} days)"
            )

            # Add all dates in range to the set
            current_date = start_date
            while current_date <= end_date:
                all_dates.add(current_date)
                current_date += timedelta(days=1)

    if not all_dates:
        logger.info("No dates to process. All bases are up to date.")
        return

    # Convert to sorted list
    dates_to_process = sorted(all_dates)

    if not dates_to_process:
        logger.info("No dates to process. Already up to date.")
        return

    logger.info(
        f"Dates to process: {dates_to_process[0]} to "
        f"{dates_to_process[-1]} ({len(dates_to_process)} days)"
    )

    # Step 3: Truncate table before processing first day
    logger.info(f"Truncating target table: {target_table}")
    truncate_query = f"TRUNCATE TABLE `{target_table}`"
    try:
        truncate_job = bq_client.query(truncate_query)
        truncate_job.result()
        logger.info("Target table truncated successfully")
    except Exception as e:
        # Table might not exist yet, which is OK for first run
        logger.warning(f"Could not truncate table (might not exist yet): {e}")
        logger.info("Table will be created on first insert")

    # Step 4: Process each date, day by day
    grand_total_rows_inserted = 0

    for process_date in dates_to_process:
        date_str = process_date.strftime("%Y-%m-%d")
        logger.info(f"Processing date: {date_str}")

        # Calculate next day for API range
        # (API requires max_date > min_date to return results)
        next_day = process_date + timedelta(days=1)
        next_day_str = next_day.strftime("%Y-%m-%d")

        # Convert to API format (DD/MM/YYYY)
        min_date_formatted = _format_date_for_api(date_str)
        max_date_formatted = _format_date_for_api(next_day_str)

        logger.info(f"API date range: {min_date_formatted} to {max_date_formatted}")

        # Process both bases for this date
        for base in bases:
            # Skip if this base has no sync history
            if last_sync_dates[base] is None:
                logger.debug(f"Skipping {base} for {date_str} (no sync history)")
                continue

            # Skip if this date is outside the base's window
            base_end_date = last_sync_dates[base]
            base_start_date = base_end_date - timedelta(days=window_days)
            if not (base_start_date <= process_date <= base_end_date):
                logger.debug(
                    f"Skipping {base} for {date_str} (outside window: "
                    f"{base_start_date} to {base_end_date})"
                )
                continue

            logger.info(f"Processing base: {base} for date: {date_str}")

            # Initial request to check total results
            initial_response = api_client.search_by_date(
                base=base,
                min_date=min_date_formatted,
                max_date=max_date_formatted,
                page=1,
            )

            total_results = initial_response.get("nbreponses", 0)
            logger.info(f"Total results for {base} on {date_str}: {total_results}")

            # Validate against API limit
            if total_results >= MAX_SEARCH_RESULTS:
                msg = (
                    f"Total results for {base} on {date_str} ({total_results}) exceeds "
                    f"API limit ({MAX_SEARCH_RESULTS}). Cannot process this date."
                )
                raise ValueError(msg)

            if total_results == 0:
                logger.info(f"No results found for {base} on {date_str}")
                continue

            # Calculate total pages
            total_pages = calculate_total_pages(total_results, results_per_page)
            logger.info(
                f"Processing {total_results} {base} results across {total_pages} pages"
            )

            # Phase 1: Collect all gencods from all pages
            all_gencods = []

            for page in range(1, total_pages + 1):
                logger.info(f"Fetching {base} page {page}/{total_pages} for {date_str}")

                try:
                    # Call search API
                    response = api_client.search_by_date(
                        base=base,
                        min_date=min_date_formatted,
                        max_date=max_date_formatted,
                        page=page,
                        results_per_page=results_per_page,
                    )

                    # Extract results
                    results = response.get("result", [])

                    if not results:
                        logger.warning(
                            f"Page {page} returned no results. Moving to next page."
                        )
                        continue

                    # Extract gencods from search results with date filter
                    page_gencods = extract_gencods_from_search_response(
                        response, from_date=min_date_formatted
                    )
                    all_gencods.extend(page_gencods)

                    logger.info(
                        f"{base} page {page} complete: "
                        f"{len(page_gencods)} gencods extracted"
                    )

                except Exception as e:
                    logger.error(
                        f"Error fetching {base} page {page} for {date_str}: {e}"
                    )
                    logger.error("Continuing to next page...")
                    continue

            # Phase 2: Fetch detailed data using /ean endpoint (same as run_init)
            if all_gencods:
                # Remove duplicates
                unique_gencods = list(set(all_gencods))
                logger.info(
                    f"Collected {len(unique_gencods)} unique gencods "
                    f"from {total_pages} pages for {base} on {date_str}"
                )

                # Map base to subcategoryid for proper routing in process_eans_batch
                # Music EANs need a music subcategoryid to be routed to music group
                # Paper EANs use None (defaults to paper group)
                if base == TiteliveCategory.MUSIC:
                    representative_subcategoryid = "SUPPORT_PHYSIQUE_MUSIQUE_CD"
                else:
                    representative_subcategoryid = None

                # Create ean_pairs with appropriate subcategoryid
                ean_pairs = [
                    (gencod, representative_subcategoryid) for gencod in unique_gencods
                ]

                # Process using shared logic from run_init
                logger.info(
                    f"Fetching detailed data for {len(ean_pairs)} EANs "
                    f"via /ean endpoint"
                )

                results = process_eans_batch(
                    api_client, ean_pairs, sub_batch_size=DEFAULT_BATCH_SIZE
                )

                # Add required fields for schema compatibility
                current_time = datetime.now()
                for result in results:
                    result["processed_at"] = current_time
                    result["batch_number"] = 0
                    result["images_download_status"] = None
                    result["images_download_processed_at"] = None
                    result["recto_image_uuid"] = None
                    result["verso_image_uuid"] = None

                # Convert to DataFrame and insert
                combined_df = pd.DataFrame(results)
                schema = get_destination_table_schema()
                insert_dataframe(
                    bq_client,
                    target_table,
                    combined_df,
                    mode="append",
                    schema=schema,
                )

                rows_inserted = len(combined_df)
                grand_total_rows_inserted += rows_inserted

                logger.info(
                    f"Completed {base} for {date_str}: Inserted {rows_inserted} rows"
                )
            else:
                logger.warning(f"No gencods collected for {base} on {date_str}")

        logger.info(f"Completed processing date: {date_str}")

    # Step 5: Deduplicate the destination table
    logger.info("Step 5: Deduplicating destination table by EAN")
    deduplicate_table_by_ean(bq_client, target_table)

    logger.info(
        f"Incremental mode complete. Inserted {grand_total_rows_inserted} "
        f"total rows to {target_table} (after deduplication)"
    )


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
