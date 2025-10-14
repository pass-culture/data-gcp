"""Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint."""

from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from src.api.auth import TokenManager
from src.api.client import TiteliveClient
from src.constants import (
    DEFAULT_BATCH_SIZE,
    GCP_PROJECT_ID,
    MAIN_BATCH_SIZE,
)
from src.transformers.api_transform import transform_api_response
from src.utils.bigquery import (
    create_destination_table,
    fetch_batch_eans,
    get_destination_table_schema,
    get_last_batch_number,
    insert_dataframe,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


def process_eans_batch(
    api_client: TiteliveClient,
    eans: list[str],
    sub_batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[dict]:
    """
    Process a batch of EANs by calling the API in sub-batches.

    Processes EANs in sub-batches of sub_batch_size (default 250).
    For each sub-batch:
    - Call API with EANs
    - Mark returned EANs as 'processed'
    - Mark missing EANs as 'deleted_in_titelive'
    - Mark failed API calls as 'fail'

    Args:
        api_client: Titelive API client
        eans: List of EANs to process
        sub_batch_size: Number of EANs per API call (default 250)

    Returns:
        List of dicts with keys: ean, status, datemodification, json_raw
        Status values: 'processed' | 'deleted_in_titelive' | 'fail'
    """
    results = []
    total_eans = len(eans)
    total_sub_batches = (total_eans + sub_batch_size - 1) // sub_batch_size

    logger.info(
        f"Processing {total_eans} EANs in {total_sub_batches} sub-batches "
        f"of {sub_batch_size} EANs each"
    )

    for i in range(0, len(eans), sub_batch_size):
        sub_batch = eans[i : i + sub_batch_size]
        current_sub_batch = (i // sub_batch_size) + 1
        eans_processed_so_far = i
        eans_in_this_batch = len(sub_batch)

        logger.info(
            f"Sub-batch {current_sub_batch}/{total_sub_batches}: "
            f"Processing EANs {eans_processed_so_far + 1}-"
            f"{eans_processed_so_far + eans_in_this_batch} "
            f"of {total_eans}"
        )

        try:
            # Call API
            api_response = api_client.get_by_eans(sub_batch)

            # Transform response
            transformed_df = transform_api_response(api_response)

            # Identify returned vs missing EANs
            returned_eans = (
                set(transformed_df["ean"].tolist())
                if not transformed_df.empty
                else set()
            )
            missing_eans = set(sub_batch) - returned_eans

            # Add processed results
            for _, row in transformed_df.iterrows():
                results.append(
                    {
                        "ean": row["ean"],
                        "status": "processed",
                        "datemodification": row["datemodification"],
                        "json_raw": row["json_raw"],
                    }
                )

            # Add deleted results
            for ean in missing_eans:
                results.append(
                    {
                        "ean": ean,
                        "status": "deleted_in_titelive",
                        "datemodification": None,
                        "json_raw": None,
                    }
                )

            logger.info(
                f"Sub-batch {current_sub_batch}/{total_sub_batches} complete: "
                f"{len(returned_eans)} processed, "
                f"{len(missing_eans)} deleted | "
                "Progress: "
                f"{eans_processed_so_far + eans_in_this_batch}/{total_eans} EANs"
            )

        except Exception as e:
            # Mark all EANs in sub-batch as failed
            logger.error(
                "API call failed for sub-batch "
                f"{current_sub_batch}/{total_sub_batches}: {e}"
            )
            for ean in sub_batch:
                results.append(
                    {
                        "ean": ean,
                        "status": "fail",
                        "datemodification": None,
                        "json_raw": None,
                    }
                )
            logger.warning(
                f"Marked {len(sub_batch)} EANs as failed | "
                "Progress: "
                f"{eans_processed_so_far + eans_in_this_batch}/{total_eans} EANs"
            )

    return results


def run_init_bq(
    source_table: str,
    destination_table: str,
    main_batch_size: int = MAIN_BATCH_SIZE,
    sub_batch_size: int = DEFAULT_BATCH_SIZE,
    project_id: str = GCP_PROJECT_ID,
    resume: bool = False,
    skip_already_processed_table: str | None = None,
    skip_count: int = 0,
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

    Optional: Skip already-processed EANs from a previous run
    - If skip_already_processed_table is provided, uses ORDER BY to sort
      already-processed EANs first, then applies OFFSET to skip them
    - skip_count should equal the number of EANs in skip_already_processed_table
    - Example: skip_count=861488 means batch 0 starts at OFFSET 861488

    Args:
        source_table: Full table ID for source (project.dataset.table)
        destination_table: Full table ID for destination (project.dataset.table)
        main_batch_size: Number of EANs per batch (default 20,000)
        sub_batch_size: Number of EANs per API call (default 250)
        project_id: GCP project ID
        resume: If True, skip table creation and resume from last batch_number
        skip_already_processed_table: Optional table containing \
            already-processed EANs to skip
        skip_count: Number of already-processed EANs to skip \
            (required if skip_already_processed_table is set)

    Raises:
        Exception: If any step fails
    """
    logger.info("Starting Mode 1: BigQuery batch processing (batch_number tracking)")
    logger.info(f"Source: {source_table}, Destination: {destination_table}")
    logger.info(f"Main batch size: {main_batch_size}, Sub-batch size: {sub_batch_size}")

    if skip_already_processed_table:
        logger.info(
            f"Skip mode: Will skip {skip_count} already-processed EANs from "
            f"{skip_already_processed_table}"
        )

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    token_manager = TokenManager(project_id)
    api_client = TiteliveClient(token_manager)

    if not resume:
        # Create destination table if not resuming
        logger.info("Step 1: Creating destination table")
        create_destination_table(bq_client, destination_table, drop_if_exists=True)

    # Step 2: Get last batch number (ONCE - critical for avoiding buffer issues)
    logger.info("Step 2: Getting last batch number from destination table")
    last_batch_number = get_last_batch_number(bq_client, destination_table)
    current_batch = last_batch_number + 1

    logger.info(f"Starting from batch {current_batch}")

    # Step 3: Process batches
    total_processed = 0

    while True:
        # Fetch EANs for this batch using OFFSET pagination
        batch_eans = fetch_batch_eans(
            bq_client,
            source_table,
            current_batch,
            main_batch_size,
            skip_already_processed_table=skip_already_processed_table,
            skip_count=skip_count,
        )

        if not batch_eans:
            logger.info("No more EANs to process. Exiting.")
            break

        logger.info(
            f"Batch {current_batch}: Processing {len(batch_eans)} EANs "
            f"in sub-batches of {sub_batch_size}"
        )

        # Process EANs in sub-batches of 250
        results = process_eans_batch(api_client, batch_eans, sub_batch_size)

        # Count statuses
        status_counts = {}
        for result in results:
            status = result["status"]
            status_counts[status] = status_counts.get(status, 0) + 1

        logger.info(
            f"Batch {current_batch} processed: "
            f"{status_counts.get('processed', 0)} processed, "
            f"{status_counts.get('deleted_in_titelive', 0)} deleted, "
            f"{status_counts.get('fail', 0)} failed"
        )

        # Add processed_at and batch_number to all results
        current_time = datetime.now()
        for result in results:
            result["processed_at"] = current_time
            result["batch_number"] = current_batch

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

        total_processed += len(batch_eans)
        logger.info(
            f"Batch {current_batch} complete. Total processed: {total_processed}"
        )

        # Increment batch number in memory (NO re-query - critical!)
        current_batch += 1

    logger.info(
        f"Mode 1 complete. Processed {total_processed} EANs total "
        f"in {current_batch - last_batch_number - 1} batches."
    )
