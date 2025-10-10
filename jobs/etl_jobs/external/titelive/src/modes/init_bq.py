"""Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint."""

from google.cloud import bigquery

from src.api.auth import TokenManager
from src.api.client import TiteliveClient
from src.constants import DEFAULT_BATCH_SIZE, GCP_PROJECT_ID
from src.transformers.api_transform import transform_api_response
from src.utils.bigquery import (
    create_target_table,
    create_tracking_table_from_source,
    get_target_table_schema,
    get_unprocessed_eans,
    insert_dataframe,
    update_ean_statuses,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_init_bq(
    source_table: str,
    tracking_table: str,
    target_table: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    project_id: str = GCP_PROJECT_ID,
    resume: bool = False,
) -> None:
    """
    Execute Mode 1: BigQuery batch processing.

    Workflow:
    1. Extract EANs from source BigQuery table
    2. Create/recreate tracking table
    3. Insert all EANs with processed=FALSE
    4. Loop: Fetch unprocessed EANs in batches
    5. Call /ean endpoint with pipe-separated EANs
    6. Transform response
    7. Load to target table
    8. Update tracking table (processed=TRUE)

    Args:
        source_table: Full table ID for source (project.dataset.table)
        tracking_table: Full table ID for tracking (project.dataset.table)
        target_table: Full table ID for target (project.dataset.table)
        batch_size: Number of EANs to process per batch
        project_id: GCP project ID
        resume: If True, skip table creation and resume from existing tracking table.
                Used to recover from service interruptions.

    Raises:
        Exception: If any step fails
    """
    logger.info("Starting Mode 1: BigQuery batch processing")
    logger.info(
        f"Source: {source_table}, Tracking: {tracking_table}, Target: {target_table}"
    )

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    token_manager = TokenManager(project_id)
    api_client = TiteliveClient(token_manager)

    if resume:
        # Resume mode: skip table creation, use existing tracking table
        logger.info("Resume mode: Using existing tracking and target tables")
        logger.info(f"Tracking table: {tracking_table}")
        logger.info(f"Target table: {target_table}")

        # Get count from existing tracking table
        from src.utils.bigquery import get_tracking_table_count

        total_eans = get_tracking_table_count(bq_client, tracking_table)

        if total_eans == 0:
            logger.warning("No EANs found in tracking table. Exiting.")
            return
    else:
        # Normal mode: create tables from scratch
        # Step 1: Create target table with proper schema
        logger.info("Step 1: Creating target table")
        create_target_table(bq_client, target_table, drop_if_exists=True)

        # Step 2: Create tracking table directly from source using SQL
        logger.info("Step 2: Creating tracking table from source (SQL)")
        total_eans = create_tracking_table_from_source(
            bq_client, tracking_table, source_table, drop_if_exists=True
        )

        if total_eans == 0:
            logger.warning("No EANs found in source table. Exiting.")
            return

    # Step 3: Process batches
    logger.info(f"Step 3: Processing {total_eans} EANs in batches of {batch_size}")

    total_processed = 0
    batch_num = 0

    while True:
        # Get next batch of unprocessed EANs
        batch_eans = get_unprocessed_eans(bq_client, tracking_table, batch_size)

        if not batch_eans:
            logger.info("No more unprocessed EANs. Batch processing complete.")
            break

        batch_num += 1
        logger.info(f"Processing batch {batch_num}: {len(batch_eans)} EANs")

        try:
            # Call API
            api_response = api_client.get_by_eans(batch_eans)

            # Transform response to 3-column format
            transformed_df = transform_api_response(api_response)

            # Identify EANs that were returned vs missing (deleted in Titelive)
            returned_eans = (
                set(transformed_df["ean"].tolist())
                if not transformed_df.empty
                else set()
            )
            missing_eans = list(set(batch_eans) - returned_eans)

            # Handle returned EANs
            if not transformed_df.empty:
                # Load to target table with explicit schema
                schema = get_target_table_schema()
                insert_dataframe(
                    bq_client,
                    target_table,
                    transformed_df,
                    mode="append",
                    schema=schema,
                )

                logger.info(f"Batch {batch_num}: Loaded {len(returned_eans)} EANs")

            # Update tracking table for both returned and missing EANs
            update_ean_statuses(
                bq_client,
                tracking_table,
                processed_eans=list(returned_eans) if returned_eans else None,
                deleted_eans=missing_eans if missing_eans else None,
            )

            if missing_eans:
                logger.info(
                    f"Batch {batch_num}: Marked {len(missing_eans)} missing EANs delete"
                )

            total_processed += len(batch_eans)
            logger.info(
                f"Batch {batch_num} complete. Processed: {total_processed}/{total_eans}"
            )

        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {e}")
            logger.error("Batch will remain unprocessed for retry. Continuing...")
            continue

    logger.info(f"Mode 1 complete. Processed {total_processed} EANs total.")
