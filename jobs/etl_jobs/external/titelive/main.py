"""Main CLI entry point for Titelive ETL pipeline."""

import typer

from config import (
    DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
    DEFAULT_SOURCE_TABLE,
    DEFAULT_TARGET_TABLE,
)
from src.constants import (
    DEFAULT_BATCH_SIZE,
    GCP_PROJECT_ID,
    MAIN_BATCH_SIZE,
    RESULTS_PER_PAGE,
)
from src.modes.download_images import run_download_images
from src.modes.init_bq import run_init_bq
from src.modes.run_incremental import run_incremental
from src.utils.logging import get_logger

logger = get_logger(__name__)

app = typer.Typer(
    help="Titelive ETL Pipeline with multiple execution modes",
    add_completion=False,
)


@app.command("init-bq")
def init_bq(
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
    skip_already_processed_table: str = typer.Option(
        None,
        "--skip-already-processed-table",
        help="Table containing already-processed EANs to skip (optional)",
    ),
    skip_count: int = typer.Option(
        0,
        "--skip-count",
        help="Number of already-processed EANs to skip \
            (required with --skip-already-processed-table)",
    ),
    reprocess_failed: bool = typer.Option(
        False,
        "--reprocess-failed",
        help="Reprocess EANs with status='failed' from destination table",
    ),
) -> None:
    """
    Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint.

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
        python main.py init-bq

        # Resume from interruption
        python main.py init-bq --resume

        # Skip already-processed EANs (e.g., from old run)
        python main.py init-bq \\
            --skip-already-processed-table "project.dataset.old_table" \\
            --skip-count 861488

        # Custom parameters
        python main.py init-bq \\
            --destination-table "project.dataset.custom_destination" \\
            --main-batch-size 10000 \\
            --sub-batch-size 250

        # Reprocess failed EANs
        python main.py init-bq --reprocess-failed
    """
    logger.info("Executing Mode 1: BigQuery batch processing (batch_number tracking)")

    try:
        run_init_bq(
            source_table=source_table,
            destination_table=destination_table,
            main_batch_size=main_batch_size,
            sub_batch_size=sub_batch_size,
            project_id=project_id,
            resume=resume,
            skip_already_processed_table=skip_already_processed_table,
            skip_count=skip_count,
            reprocess_failed=reprocess_failed,
        )
        logger.info("Mode 1 execution completed successfully")
    except Exception as e:
        logger.error(f"Mode 1 execution failed: {e}")
        raise typer.Exit(code=1) from e


@app.command("run")
def run(
    min_modified_date: str = typer.Option(
        ...,
        "--min-modified-date",
        help="Minimum modification date (YYYY-MM-DD)",
    ),
    max_modified_date: str = typer.Option(
        ...,
        "--max-modified-date",
        help="Maximum modification date (YYYY-MM-DD)",
    ),
    base: str = typer.Option(
        ...,
        "--base",
        help="Product category (e.g., 'paper', 'music')",
    ),
    target_table: str = typer.Option(
        ...,
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
    Mode 3: Incremental search by date range with pagination.

    This mode:
    1. Searches products by modification date range
    2. Validates total results against API limit (20,000)
    3. Paginates through results
    4. Transforms and appends to target table

    Example:

        python main.py run \\
        --min-modified-date "2025-10-08" \\
        --max-modified-date "2025-10-08" \\
        --base "paper" \\
        --target-table "passculture-data-ehp.tmp_cdarnis_dev.tmp_titelive__products" \\
        --results-per-page 120
    """
    logger.info("Executing Mode 3: Incremental date range search")

    try:
        run_incremental(
            min_modified_date=min_modified_date,
            max_modified_date=max_modified_date,
            base=base,
            target_table=target_table,
            results_per_page=results_per_page,
            project_id=project_id,
        )
        logger.info("Mode 3 execution completed successfully")
    except Exception as e:
        logger.error(f"Mode 3 execution failed: {e}")
        raise typer.Exit(code=1) from e


@app.command("download-images")
def download_images(
    source_table: str = typer.Option(
        DEFAULT_TARGET_TABLE,
        "--source-table",
        help="Source BigQuery table with batch_number and \
        json_raw (project.dataset.table)",
    ),
    gcs_bucket: str = typer.Option(
        DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
        "--gcs-bucket",
        help="Target GCS bucket name (without gs://)",
    ),
    gcs_prefix: str = typer.Option(
        "images",
        "--gcs-prefix",
        help="Prefix for GCS paths (e.g., 'images/titelive')",
    ),
    max_concurrent: int = typer.Option(
        100,
        "--max-concurrent",
        help="Maximum concurrent downloads (default: 100)",
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        help="Number of EANs per batch for reprocess-failed mode (default: 1000)",
    ),
    reprocess_failed: bool = typer.Option(
        False,
        "--reprocess-failed",
        help="Reprocess EANs with images_download_status='failed'",
    ),
) -> None:
    """
    Mode 4: Download images from BigQuery table and upload to GCS.

    Uses batch_number tracking from source table for progress management:
    1. Iterates through batches starting from batch 0
    2. For each batch N:
       - Fetches EANs WHERE batch_number = N AND images_download_status IS NULL
       - Extracts recto/verso URLs from json_raw column
       - Downloads images asynchronously to GCS
       - Updates images_download_status: 'processed' or 'failed'
    3. Automatically resumes by filtering WHERE images_download_status IS NULL

    Features:
    - True async I/O with asyncio + aiohttp (no GIL bottleneck)
    - Configurable concurrency with semaphore
    - Connection pooling for HTTP requests
    - Automatic retry logic with exponential backoff
    - Batch tracking aligned with init-bq mode

    The query extracts:
    - recto: $.article[0].imagesUrl.recto
    - verso: $.article[0].imagesUrl.verso

    Project ID is loaded from GCP_PROJECT_ID constant (env var or default).

    Example:

        # Initial run (processes all pending images)
        python main.py download-images

        # Custom source table and GCS location
        python main.py download-images \\
            --source-table "project.dataset.titelive__products" \\
            --gcs-bucket "data-team-sandbox-dev" \\
            --gcs-prefix "images/titelive" \\
            --max-concurrent 100

        # Reprocess failed downloads
        python main.py download-images --reprocess-failed
    """
    logger.info("Executing Mode 4: Async image download from BigQuery to GCS")

    try:
        run_download_images(
            source_table=source_table,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            max_concurrent=max_concurrent,
            batch_size=batch_size,
            reprocess_failed=reprocess_failed,
        )
        logger.info("Mode 4 execution completed successfully")
    except Exception as e:
        logger.error(f"Mode 4 execution failed: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
