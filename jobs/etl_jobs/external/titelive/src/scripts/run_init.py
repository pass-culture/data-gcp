"""Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint."""

from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from config import (
    DEFAULT_BATCH_SIZE,
    GCP_PROJECT_ID,
    MAIN_BATCH_SIZE,
)
from src.api.auth import TokenManager
from src.api.client import TiteliveClient
from src.utils.bigquery import (
    count_failed_eans,
    create_destination_table,
    deduplicate_table_by_ean,
    delete_failed_eans,
    fetch_batch_eans,
    fetch_failed_eans,
    get_destination_table_schema,
    get_last_batch_number,
    insert_dataframe,
)
from src.utils.ean_processing import process_eans_batch
from src.utils.logging import get_logger

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
    token_manager = TokenManager(project_id)
    api_client = TiteliveClient(token_manager)

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
