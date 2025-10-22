"""Main CLI entry point for Titelive ETL pipeline."""

import typer

from config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_SOURCE_TABLE,
    DEFAULT_TARGET_TABLE,
    GCP_PROJECT_ID,
    MAIN_BATCH_SIZE,
    RESULTS_PER_PAGE,
)
from src.scripts.download_images import run_download_images
from src.scripts.run_incremental import run_incremental
from src.scripts.run_init import run_init
from src.utils.logging import get_logger

logger = get_logger(__name__)

app = typer.Typer(
    help="Titelive ETL Pipeline with multiple execution modes",
    add_completion=False,
)


@app.command("run-init")
def run_init_command(
    source_table: str = typer.Option(
        DEFAULT_SOURCE_TABLE,
        "--source-table",
        help="Source BigQuery table ID (project.dataset.table)",
    ),
    destination_table: str = typer.Option(
        DEFAULT_TARGET_TABLE,
        "--destination-table",
        help="Destination BigQuery table ID (project.dataset.table)",
    ),
    main_batch_size: int = typer.Option(
        MAIN_BATCH_SIZE,
        "--main-batch-size",
        help="Number of EANs per batch (default 20,000)",
    ),
    sub_batch_size: int = typer.Option(
        DEFAULT_BATCH_SIZE,
        "--sub-batch-size",
        help="Number of EANs per API call (max 250)",
    ),
    project_id: str = typer.Option(
        GCP_PROJECT_ID,
        "--project-id",
        help="GCP project ID",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume from last batch_number (skip table creation)",
    ),
    reprocess_failed: bool = typer.Option(
        False,
        "--reprocess-failed",
        help="Reprocess EANs with status='failed' from destination table",
    ),
) -> None:
    """
    Run Init: Extract EANs from BigQuery and batch process via /ean endpoint.

    New architecture using single destination table with batch_number tracking:
    1. Query destination_table ONCE to get last batch_number
    2. For each batch N:
       - Fetch 20k EANs from source_table using OFFSET pagination
       - Process in sub-batches of 250 EANs (API limit)
       - Handle 3 statuses: processed | deleted_in_titelive | failed
       - Write all results with batch_number = N
    3. Increment batch_number in memory (no re-query)

    This eliminates BigQuery buffer issues by querying destination_table only once.

    Default table names (from config.py):
    - Source: {PROJECT}.raw_{ENV}.applicative_database_product
    - Destination: {PROJECT}.{DATASET}.tmp_titelive__products

    Example (using defaults):

        # Initial run (creates table)
        python main.py run-init

        # Resume from interruption
        python main.py run-init --resume

        # Custom parameters
        python main.py run-init \\
            --destination-table "project.dataset.custom_destination" \\
            --main-batch-size 10000 \\
            --sub-batch-size 250

        # Reprocess failed EANs
        python main.py run-init --reprocess-failed
    """
    logger.info("Executing Run Init: BigQuery batch processing (batch_number tracking)")

    try:
        run_init(
            source_table=source_table,
            destination_table=destination_table,
            main_batch_size=main_batch_size,
            sub_batch_size=sub_batch_size,
            project_id=project_id,
            resume=resume,
            reprocess_failed=reprocess_failed,
        )
        logger.info("Run Init execution completed successfully")
    except Exception as e:
        logger.error(f"Run Init execution failed: {e}")
        raise typer.Exit(code=1) from e


@app.command("run-incremental")
def run_incremental_command(
    target_table: str = typer.Option(
        DEFAULT_TARGET_TABLE,
        "--target-table",
        help="Target BigQuery table ID (project.dataset.table)",
    ),
    results_per_page: int = typer.Option(
        RESULTS_PER_PAGE,
        "--results-per-page",
        help="Number of results per page",
    ),
    project_id: str = typer.Option(
        GCP_PROJECT_ID,
        "--project-id",
        help="GCP project ID",
    ),
) -> None:
    """
    Incremental mode: Sync products modified since last sync for both paper and music.

    This mode:
    1. Queries last sync date from provider event table for each base
    2. Processes data day-by-day from last_sync_date + 1 to yesterday
    3. Truncates table before first day, then appends for subsequent days
    4. Validates total results against API limit (20,000) per base per day
    5. Paginates through results and inserts to target table

    The sync date is tracked via the 'processed_at' field in the target table.

    Example:

        python main.py run-incremental \\
        --target-table "passculture-data-ehp.tmp_cdarnis_dev.tmp_titelive__products" \\
        --results-per-page 120
    """
    logger.info("Executing Run Incremental: Incremental sync")

    try:
        run_incremental(
            target_table=target_table,
            results_per_page=results_per_page,
            project_id=project_id,
        )
        logger.info("Run Incremental execution completed successfully")
    except Exception as e:
        logger.error(f"Run Incremental execution failed: {e}")
        raise typer.Exit(code=1) from e


@app.command("download-images")
def download_images(
    reprocess_failed: bool = typer.Option(
        False,
        "--reprocess-failed",
        help="Reprocess EANs with images_download_status='failed'",
    ),
) -> None:
    """
    Run Download Images: Download images from BigQuery table and upload to GCS.

    Unified architecture for both normal and reprocess modes:
    1. Iterates through batch_numbers (0, 1, 2, ...)
    2. For each batch_number:
       - Fetches ALL EANs for this batch (up to 20k) with matching status filter
       - Chunks into sub-batches of 1000 EANs (configurable in config.py)
       - Processes each sub-batch: downloads images using ThreadPoolExecutor
       - Accumulates all results in memory
       - Writes ALL results to BigQuery once when batch is complete
    3. Moves to next batch_number

    Status filters:
    - Normal mode: images_download_status IS NULL (pending)
    - Reprocess mode: images_download_status = 'failed' (retry)

    All configuration loaded from config.py:
    - source_table: DEFAULT_TARGET_TABLE
    - gcs_bucket: DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME
    - gcs_prefix: IMAGE_DOWNLOAD_GCS_PREFIX
    - max_workers: IMAGE_DOWNLOAD_MAX_WORKERS (default: (cpu_count - 1) * 5)
    - pool_connections: IMAGE_DOWNLOAD_POOL_CONNECTIONS (default: 10)
    - pool_maxsize: IMAGE_DOWNLOAD_POOL_MAXSIZE (default: 20)
    - sub_batch_size: IMAGE_DOWNLOAD_SUB_BATCH_SIZE (default: 1000)

    Features:
    - Threaded I/O with ThreadPoolExecutor for concurrent downloads
    - Connection pooling with requests.Session and HTTPAdapter
    - Automatic retry logic with exponential backoff (10 retries)
    - Progress bar with tqdm for visual feedback
    - Batch tracking aligned with run-init mode

    Example:

        # Process all pending images
        python main.py download-images

        # Reprocess failed downloads
        python main.py download-images --reprocess-failed
    """
    logger.info(
        "Executing Run Download Images: Threaded image download from BigQuery to GCS"
    )

    try:
        run_download_images(
            reprocess_failed=reprocess_failed,
        )
        logger.info("Run Download Images execution completed successfully")
    except Exception as e:
        logger.error(f"Run Download Images execution failed: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
