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
    MUSIC_SUBCATEGORIES,
    TiteliveCategory,
)
from src.transformers.api_transform import transform_api_response
from src.utils.bigquery import (
    count_failed_eans,
    create_destination_table,
    delete_failed_eans,
    fetch_batch_eans,
    fetch_failed_eans,
    get_destination_table_schema,
    get_last_batch_number,
    insert_dataframe,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


def process_eans_batch(
    api_client: TiteliveClient,
    ean_pairs: list[tuple[str, str]],
    sub_batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[dict]:
    """
    Process a batch of EANs by calling the API in sub-batches, grouped by base.

    Groups EANs by base (music vs paper) based on subcategoryid, then processes
    each group in sub-batches of sub_batch_size (default 250).
    For each sub-batch:
    - Call API with EANs and appropriate base parameter
    - Mark returned EANs as 'processed'
    - Mark missing EANs as 'deleted_in_titelive'
    - Mark failed API calls as 'failed'

    Args:
        api_client: Titelive API client
        ean_pairs: List of (ean, subcategoryid) tuples to process
        sub_batch_size: Number of EANs per API call (default 250)

    Returns:
        List of dicts with keys: ean, subcategoryid, status, datemodification, json_raw
        Status values: 'processed' | 'deleted_in_titelive' | 'failed'
    """
    # Group EANs by base category
    music_eans = []
    paper_eans = []

    for ean, subcategoryid in ean_pairs:
        if subcategoryid in MUSIC_SUBCATEGORIES:
            music_eans.append((ean, subcategoryid))
        else:
            # Default to paper for NULL, unknown, or paper subcategories
            paper_eans.append((ean, subcategoryid))

    total_eans = len(ean_pairs)
    logger.info(
        f"Processing {total_eans} EANs: "
        f"{len(music_eans)} music, {len(paper_eans)} paper"
    )

    results = []

    # Process music EANs
    if music_eans:
        logger.info(f"Processing {len(music_eans)} music EANs")
        results.extend(
            _process_eans_by_base(
                api_client, music_eans, TiteliveCategory.MUSIC, sub_batch_size
            )
        )

    # Process paper EANs
    if paper_eans:
        logger.info(f"Processing {len(paper_eans)} paper EANs")
        results.extend(
            _process_eans_by_base(
                api_client, paper_eans, TiteliveCategory.PAPER, sub_batch_size
            )
        )

    return results


def _process_eans_by_base(
    api_client: TiteliveClient,
    ean_pairs: list[tuple[str, str]],
    base: str,
    sub_batch_size: int,
) -> list[dict]:
    """
    Process EANs for a specific base category in sub-batches.

    Args:
        api_client: Titelive API client
        ean_pairs: List of (ean, subcategoryid) tuples
        base: API base category ('music' or 'paper')
        sub_batch_size: Number of EANs per API call

    Returns:
        List of dicts with processing results
    """
    results = []
    total_eans = len(ean_pairs)
    total_sub_batches = (total_eans + sub_batch_size - 1) // sub_batch_size

    logger.info(
        f"Processing {total_eans} {base} EANs in {total_sub_batches} sub-batches "
        f"of {sub_batch_size} EANs each"
    )

    for i in range(0, len(ean_pairs), sub_batch_size):
        sub_batch_pairs = ean_pairs[i : i + sub_batch_size]
        sub_batch_eans = [ean for ean, _ in sub_batch_pairs]
        current_sub_batch = (i // sub_batch_size) + 1
        eans_processed_so_far = i
        eans_in_this_batch = len(sub_batch_pairs)

        logger.info(
            f"Sub-batch {current_sub_batch}/{total_sub_batches} ({base}): "
            f"Processing EANs {eans_processed_so_far + 1}-"
            f"{eans_processed_so_far + eans_in_this_batch} "
            f"of {total_eans}"
        )

        try:
            # Call API with base parameter
            api_response = api_client.get_by_eans_with_base(sub_batch_eans, base)

            # Transform response
            transformed_df = transform_api_response(api_response)

            # Identify returned vs missing EANs
            returned_eans = (
                set(transformed_df["ean"].tolist())
                if not transformed_df.empty
                else set()
            )
            missing_eans = set(sub_batch_eans) - returned_eans

            # Create mapping of ean to subcategoryid for this sub-batch
            ean_to_subcategoryid = dict(sub_batch_pairs)

            # Add processed results
            for _, row in transformed_df.iterrows():
                results.append(
                    {
                        "ean": row["ean"],
                        "subcategoryid": ean_to_subcategoryid.get(row["ean"]),
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
                        "subcategoryid": ean_to_subcategoryid.get(ean),
                        "status": "deleted_in_titelive",
                        "datemodification": None,
                        "json_raw": None,
                    }
                )

            logger.info(
                f"Sub-batch {current_sub_batch}/{total_sub_batches} ({base}) complete: "
                f"{len(returned_eans)} processed, "
                f"{len(missing_eans)} deleted | "
                "Progress: "
                f"{eans_processed_so_far + eans_in_this_batch}/{total_eans} EANs"
            )

        except Exception as e:
            # Mark all EANs in sub-batch as failed
            logger.error(
                f"API call failed for sub-batch "
                f"{current_sub_batch}/{total_sub_batches} ({base}): {e}"
            )
            for ean, subcategoryid in sub_batch_pairs:
                results.append(
                    {
                        "ean": ean,
                        "subcategoryid": subcategoryid,
                        "status": "fail",
                        "datemodification": None,
                        "json_raw": None,
                    }
                )
            logger.warning(
                f"Marked {len(sub_batch_pairs)} EANs as failed | "
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

    Optional: Skip already-processed EANs from a previous run
    - If skip_already_processed_table is provided, uses ORDER BY to sort
      already-processed EANs first, then applies OFFSET to skip them
    - skip_count should equal the number of EANs in skip_already_processed_table
    - Example: skip_count=861488 means batch 0 starts at OFFSET 861488

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
        skip_already_processed_table: Optional table containing \
            already-processed EANs to skip
        skip_count: Number of already-processed EANs to skip \
            (required if skip_already_processed_table is set)
        reprocess_failed: If True, reprocess EANs with status='failed'

    Raises:
        Exception: If any step fails
    """
    logger.info("Starting Mode 1: BigQuery batch processing (batch_number tracking)")
    logger.info(f"Source: {source_table}, Destination: {destination_table}")
    logger.info(f"Main batch size: {main_batch_size}, Sub-batch size: {sub_batch_size}")

    if reprocess_failed:
        logger.info("Reprocess failed mode: Will fetch and reprocess failed EANs")
    elif skip_already_processed_table:
        logger.info(
            f"Skip mode: Will skip {skip_count} already-processed EANs from "
            f"{skip_already_processed_table}"
        )

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

            # Delete failed records before reprocessing (extract EANs only)
            batch_eans_only = [ean for ean, _ in batch_ean_pairs]
            delete_failed_eans(bq_client, destination_table, batch_eans_only)
        else:
            # Fetch EANs from source table using OFFSET pagination (returns tuples)
            batch_ean_pairs = fetch_batch_eans(
                bq_client,
                source_table,
                current_batch,
                main_batch_size,
                skip_already_processed_table=skip_already_processed_table,
                skip_count=skip_count,
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

    logger.info(
        f"Mode 1 complete. Processed {total_processed} EANs total "
        f"in {current_batch - last_batch_number - 1} batches."
    )
