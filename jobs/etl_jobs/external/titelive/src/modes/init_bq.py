"""Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint."""

from collections import deque
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from src.api.auth import TokenManager
from src.api.client import TiteliveClient
from src.constants import DEFAULT_BATCH_SIZE, FLUSH_THRESHOLD, GCP_PROJECT_ID
from src.transformers.api_transform import transform_api_response
from src.utils.bigquery import (
    create_processed_eans_table,
    create_target_table,
    create_tracking_table_from_source,
    get_target_table_schema,
    get_tracking_table_count,
    get_unprocessed_eans,
    insert_dataframe,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_init_bq(
    source_table: str,
    tracking_table: str,
    processed_eans_table: str,
    target_table: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    flush_threshold: int = FLUSH_THRESHOLD,
    project_id: str = GCP_PROJECT_ID,
    resume: bool = False,
) -> None:
    """
    Execute Mode 1: BigQuery batch processing with buffered inserts.

    Optimized workflow to avoid UPDATE quota limits:
    1. Extract EANs from source BigQuery table
    2. Create tracking table (immutable, EANs only)
    3. Create processed_eans table (append-only)
    4. Loop: Fetch unprocessed EANs using LEFT JOIN
    5. Call /ean endpoint with pipe-separated EANs
    6. Accumulate results in memory buffers
    7. Flush to BigQuery every flush_threshold EANs
    8. Final flush for remaining EANs

    Args:
        source_table: Full table ID for source (project.dataset.table)
        tracking_table: Full table ID for tracking (project.dataset.table)
        processed_eans_table: Full table ID for processed_eans (project.dataset.table)
        target_table: Full table ID for target (project.dataset.table)
        batch_size: Number of EANs to process per API batch (max 250)
        flush_threshold: Flush to BigQuery every N EANs (default 20,000)
        project_id: GCP project ID
        resume: If True, skip table creation and resume from existing tables.
                Used to recover from service interruptions.

    Raises:
        Exception: If any step fails
    """
    logger.info("Starting Mode 1: BigQuery batch processing (optimized)")
    logger.info(
        f"Source: {source_table}, Tracking: {tracking_table}, "
        f"Processed: {processed_eans_table}, Target: {target_table}"
    )
    logger.info(f"Batch size: {batch_size}, Flush threshold: {flush_threshold}")

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    token_manager = TokenManager(project_id)
    api_client = TiteliveClient(token_manager)

    if resume:
        # Resume mode: skip table creation, use existing tables
        logger.info("Resume mode: Using existing tables")
        logger.info(f"Tracking table: {tracking_table}")
        logger.info(f"Processed EANs table: {processed_eans_table}")
        logger.info(f"Target table: {target_table}")

        # Get count from existing tracking table
        total_eans = get_tracking_table_count(
            bq_client, tracking_table, processed_eans_table
        )

        if total_eans == 0:
            logger.warning("No unprocessed EANs found. Exiting.")
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

        # Step 3: Create processed_eans table
        logger.info("Step 3: Creating processed_eans table")
        create_processed_eans_table(
            bq_client, processed_eans_table, drop_if_exists=True
        )

    # Step 4: Process batches with memory buffering
    logger.info(
        f"Step 4: Processing {total_eans} EANs in batches of {batch_size} "
        f"(flush every {flush_threshold} EANs)"
    )

    # Initialize memory buffers
    processed_buffer = []  # Accumulate processed EAN records
    results_buffer = []  # Accumulate API response dataframes
    already_fetched = set()  # Track EANs in current buffer
    ean_history = deque(maxlen=2)  # Sliding window: keep last 2 flush cycles

    total_processed = 0
    batch_num = 0
    flush_count = 0

    while True:
        # Get next batch of unprocessed EANs (excluding in-memory buffer + history)
        exclude_eans = (
            already_fetched.union(*ean_history) if ean_history else already_fetched
        )
        batch_eans = get_unprocessed_eans(
            bq_client,
            tracking_table,
            processed_eans_table,
            batch_size,
            exclude_eans=exclude_eans,
        )

        if not batch_eans:
            logger.info("No more unprocessed EANs.")
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
            missing_eans = set(batch_eans) - returned_eans

            # Accumulate in memory buffers
            current_time = datetime.now()

            # Add returned EANs to processed buffer
            processed_buffer.extend(
                [
                    {
                        "ean": ean,
                        "processed_at": current_time,
                        "status": "processed",
                    }
                    for ean in returned_eans
                ]
            )

            # Add missing EANs to processed buffer
            processed_buffer.extend(
                [
                    {
                        "ean": ean,
                        "processed_at": current_time,
                        "status": "deleted_in_titelive",
                    }
                    for ean in missing_eans
                ]
            )

            # Add results to buffer
            if not transformed_df.empty:
                results_buffer.append(transformed_df)

            # Track fetched EANs
            already_fetched.update(batch_eans)

            total_processed += len(batch_eans)

            logger.info(
                f"Batch {batch_num}: {len(returned_eans)} returned, "
                f"{len(missing_eans)} deleted. "
                f"Buffer: {len(processed_buffer)} EANs"
            )

            # Flush to BigQuery when threshold reached
            if len(processed_buffer) >= flush_threshold:
                flush_count += 1
                logger.info(
                    f"Flush {flush_count}: Writing {len(processed_buffer)}"
                    "EANs to BigQuery"
                )

                # Flush processed_eans
                processed_df = pd.DataFrame(processed_buffer)
                insert_dataframe(
                    bq_client,
                    processed_eans_table,
                    processed_df,
                    mode="append",
                )

                # Flush target table results
                if results_buffer:
                    combined_df = pd.concat(results_buffer, ignore_index=True)
                    schema = get_target_table_schema()
                    insert_dataframe(
                        bq_client,
                        target_table,
                        combined_df,
                        mode="append",
                        schema=schema,
                    )
                    logger.info(
                        f"Flush {flush_count}: Wrote {len(combined_df)} rows to target"
                    )

                # Clear buffers (save history for sliding window protection)
                processed_buffer = []
                results_buffer = []
                ean_history.append(already_fetched.copy())
                already_fetched.clear()

                logger.info(
                    f"Flush {flush_count} complete. "
                    f"Progress: {total_processed}/{total_eans}"
                )

        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {e}")
            logger.error("Batch will remain unprocessed for retry. Continuing...")
            continue

    # Final flush for remaining EANs in buffer
    if processed_buffer:
        flush_count += 1
        logger.info(
            f"Final flush {flush_count}: Writing {len(processed_buffer)} remaining EANs"
        )

        processed_df = pd.DataFrame(processed_buffer)
        insert_dataframe(
            bq_client,
            processed_eans_table,
            processed_df,
            mode="append",
        )

        if results_buffer:
            combined_df = pd.concat(results_buffer, ignore_index=True)
            schema = get_target_table_schema()
            insert_dataframe(
                bq_client,
                target_table,
                combined_df,
                mode="append",
                schema=schema,
            )
            logger.info(f"Final flush: Wrote {len(combined_df)} rows to target")

    logger.info(
        f"Mode 1 complete. Processed {total_processed} EANs total "
        f"with {flush_count} BigQuery flushes."
    )
