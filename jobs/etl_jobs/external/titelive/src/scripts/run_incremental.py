"""Mode 3: Incremental search by date range with pagination."""

from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery

from config import (
    GCP_PROJECT_ID,
    MAX_SEARCH_RESULTS,
    PROVIDER_EVENT_TABLE,
    RESULTS_PER_PAGE,
    TITELIVE_PROVIDER_ID,
    TiteliveCategory,
)
from src.api.auth import TokenManager
from src.api.client import TiteliveClient
from src.utils.api_transform import transform_api_response
from src.utils.batching import calculate_total_pages
from src.utils.bigquery import (
    get_destination_table_schema,
    get_last_sync_date,
    insert_dataframe,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_incremental(
    target_table: str,
    results_per_page: int = RESULTS_PER_PAGE,
    project_id: str = GCP_PROJECT_ID,
    provider_event_table: str = PROVIDER_EVENT_TABLE,
    provider_id: str = TITELIVE_PROVIDER_ID,
) -> None:
    """
    Execute incremental date range search for both paper and music products.

    Workflow:
    1. Query last sync date for each base from provider event table
    2. Generate list of dates to process (last_sync_date + 1 to today - 1)
    3. Truncate table before processing first day
    4. For each date and each base ("paper" and "music"):
       - Initial search request to check total results
       - Verify nbresults < 20000 (API limit)
       - Calculate total pages
       - Iterate through pages
       - Transform and append results to target table

    Args:
        target_table: Full table ID for target (project.dataset.table)
        results_per_page: Number of results per page (default: 120)
        project_id: GCP project ID
        provider_event_table: Full table ID for provider event table
        provider_id: Provider ID for Titelive

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
        last_sync_date = get_last_sync_date(
            bq_client, provider_event_table, provider_id, base
        )
        if last_sync_date:
            last_sync_dates[base] = datetime.strptime(last_sync_date, "%Y-%m-%d").date()
        else:
            # If no previous sync, we'll skip this base
            logger.warning(f"No previous sync found for {base}, skipping")
            last_sync_dates[base] = None

    # Step 2: Generate list of dates to process
    today = datetime.now().date()
    end_date = today - timedelta(days=1)  # Process up to yesterday

    # Find the earliest start date across all bases
    start_dates = []
    for base in bases:
        if last_sync_dates[base] is not None:
            start_date = last_sync_dates[base] + timedelta(days=1)
            if start_date <= end_date:
                start_dates.append(start_date)

    if not start_dates:
        logger.info("No dates to process. All bases are up to date.")
        return

    # Use the earliest start date to ensure we process all necessary dates
    earliest_start_date = min(start_dates)

    # Generate list of dates
    dates_to_process = []
    current_date = earliest_start_date
    while current_date <= end_date:
        dates_to_process.append(current_date)
        current_date += timedelta(days=1)

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

        # Process both bases for this date
        for base in bases:
            # Skip if this base doesn't need this date
            if last_sync_dates[base] is None or process_date <= last_sync_dates[base]:
                logger.debug(
                    f"Skipping {base} for {date_str} "
                    "(already synced or no sync history)"
                )
                continue

            logger.info(f"Processing base: {base} for date: {date_str}")

            # Initial request to check total results
            initial_response = api_client.search_by_date(
                base=base,
                min_date=min_date_formatted,
                max_date=max_date_formatted,
                page=1,
                results_per_page=0,  # Get metadata only
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

            # Iterate through pages and collect all dataframes
            all_dataframes = []

            for page in range(1, total_pages + 1):
                logger.info(
                    f"Processing {base} page {page}/{total_pages} for {date_str}"
                )

                try:
                    # Call API
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

                    # Transform response
                    transformed_df = transform_api_response(response)

                    # Add required fields for schema compatibility
                    if not transformed_df.empty:
                        transformed_df["status"] = "processed"
                        transformed_df["processed_at"] = datetime.now()
                        transformed_df["batch_number"] = 0
                        transformed_df["subcategoryid"] = None
                        transformed_df["images_download_status"] = None
                        transformed_df["images_download_processed_at"] = None
                        all_dataframes.append(transformed_df)
                        logger.info(
                            f"{base} page {page} complete: "
                            f"{len(transformed_df)} rows fetched"
                        )
                    else:
                        logger.warning(
                            f"Page {page} produced no data after transformation."
                        )

                except Exception as e:
                    logger.error(
                        f"Error processing {base} page {page} for {date_str}: {e}"
                    )
                    logger.error("Continuing to next page...")
                    continue

            # Insert all data at once after fetching all pages
            if all_dataframes:
                combined_df = pd.concat(all_dataframes, ignore_index=True)

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
                    f"Completed {base} for {date_str}: Inserted {rows_inserted} "
                    f"rows from {len(all_dataframes)} pages"
                )
            else:
                logger.warning(f"No data to insert for {base} on {date_str}")

        logger.info(f"Completed processing date: {date_str}")

    logger.info(
        f"Incremental mode complete. Inserted {grand_total_rows_inserted} "
        f"total rows to {target_table}"
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
