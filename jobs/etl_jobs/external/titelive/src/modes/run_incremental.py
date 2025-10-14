"""Mode 3: Incremental search by date range with pagination."""

from datetime import datetime

from google.cloud import bigquery

from src.api.auth import TokenManager
from src.api.client import TiteliveClient
from src.constants import GCP_PROJECT_ID, MAX_SEARCH_RESULTS, RESULTS_PER_PAGE
from src.transformers.api_transform import transform_api_response
from src.utils.batching import calculate_total_pages
from src.utils.bigquery import get_target_table_schema, insert_dataframe
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_incremental(
    min_modified_date: str,
    max_modified_date: str,
    base: str,
    target_table: str,
    results_per_page: int = RESULTS_PER_PAGE,
    project_id: str = GCP_PROJECT_ID,
) -> None:
    """
    Execute Mode 3: Incremental date range search.

    Workflow:
    1. Initial search request to check total results
    2. Verify nbresults < 20000 (API limit)
    3. Calculate total pages
    4. Iterate through pages:
       - Call /search endpoint
       - Transform response
       - Append to target table

    Args:
        min_modified_date: Minimum modification date (YYYY-MM-DD)
        max_modified_date: Maximum modification date (YYYY-MM-DD)
        base: Product category (e.g., "paper", "music")
        target_table: Full table ID for target (project.dataset.table)
        results_per_page: Number of results per page (default: 120)
        project_id: GCP project ID

    Raises:
        ValueError: If total results exceed API limit
        Exception: If any step fails
    """
    logger.info("Starting Mode 3: Incremental date range search")
    logger.info(f"Date range: {min_modified_date} to {max_modified_date}")
    logger.info(f"Base: {base}, Target: {target_table}")

    # Convert dates to DD/MM/YYYY format for API
    min_date_formatted = _format_date_for_api(min_modified_date)
    max_date_formatted = _format_date_for_api(max_modified_date)

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    token_manager = TokenManager(project_id)
    api_client = TiteliveClient(token_manager)

    # Step 1: Initial request to check total results
    logger.info("Step 1: Checking total results")

    initial_response = api_client.search_by_date(
        base=base,
        min_date=min_date_formatted,
        max_date=max_date_formatted,
        page=1,
        results_per_page=0,  # Get metadata only
    )

    total_results = initial_response.get("nbreponses", 0)
    logger.info(f"Total results: {total_results}")

    # Step 2: Validate against API limit
    if total_results >= MAX_SEARCH_RESULTS:
        msg = (
            f"Total results ({total_results}) exceeds API limit ({MAX_SEARCH_RESULTS})"
            "Please narrow the date range."
        )
        raise ValueError(msg)

    if total_results == 0:
        logger.warning("No results found for given criteria. Exiting.")
        return

    # Step 3: Calculate total pages
    total_pages = calculate_total_pages(total_results, results_per_page)
    logger.info(
        f"Step 3: Processing {total_results} results across {total_pages} pages"
    )

    # Step 4: Iterate through pages
    total_rows_inserted = 0

    for page in range(1, total_pages + 1):
        logger.info(f"Processing page {page}/{total_pages}")

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
                logger.warning(f"Page {page} returned no results. Moving to next page.")
                continue

            # Transform response
            transformed_df = transform_api_response(response)

            if not transformed_df.empty:
                # Insert to target table with explicit schema
                schema = get_target_table_schema()
                insert_dataframe(
                    bq_client,
                    target_table,
                    transformed_df,
                    mode="append",
                    schema=schema,
                )

                rows_inserted = len(transformed_df)
                total_rows_inserted += rows_inserted

                logger.info(
                    f"Page {page} complete: {rows_inserted} rows inserted. "
                    f"Total: {total_rows_inserted}"
                )
            else:
                logger.warning(f"Page {page} produced no data after transformation.")

        except Exception as e:
            logger.error(f"Error processing page {page}: {e}")
            logger.error("Continuing to next page...")
            continue

    logger.info(
        f"Mode 3 complete. Inserted {total_rows_inserted} total rows to {target_table}"
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
