"""Main CLI entry point for Titelive ETL pipeline."""

import typer

from config import (
    DEFAULT_PROCESSED_EANS_TABLE,
    DEFAULT_SOURCE_TABLE,
    DEFAULT_TARGET_TABLE,
    DEFAULT_TRACKING_TABLE,
)
from src.constants import DEFAULT_BATCH_SIZE, GCP_PROJECT_ID, RESULTS_PER_PAGE
from src.modes.init_bq import run_init_bq
from src.modes.init_gcs import run_init_gcs
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
    tracking_table: str = typer.Option(
        DEFAULT_TRACKING_TABLE,
        "--tracking-table",
        help="Tracking BigQuery table ID (project.dataset.table)",
    ),
    processed_eans_table: str = typer.Option(
        DEFAULT_PROCESSED_EANS_TABLE,
        "--processed-eans-table",
        help="Processed EANs BigQuery table ID (project.dataset.table)",
    ),
    target_table: str = typer.Option(
        DEFAULT_TARGET_TABLE,
        "--target-table",
        help="Target BigQuery table ID (project.dataset.table)",
    ),
    batch_size: int = typer.Option(
        DEFAULT_BATCH_SIZE,
        "--batch-size",
        help="Number of EANs to process per API batch (max 250)",
    ),
    project_id: str = typer.Option(
        GCP_PROJECT_ID,
        "--project-id",
        help="GCP project ID",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume from existing tables (skip table creation)",
    ),
) -> None:
    """
    Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint (optimized).

    This mode uses buffered inserts to avoid BigQuery UPDATE quota limits:
    1. Extracts EANs from source table
    2. Creates tracking table (immutable, EANs only)
    3. Creates processed_eans table (append-only)
    4. Fetches product data in batches via API
    5. Accumulates results in memory
    6. Flushes to BigQuery every 20K EANs
    7. Supports resume from interruption

    Default table names (from config.py):
    - Source: {PROJECT}.raw_{ENV}.applicative_database_product
    - Tracking: {PROJECT}.{DATASET}.tmp_titelive__tracking
    - Processed EANs: {PROJECT}.{DATASET}.tmp_titelive__processed_eans
    - Target: {PROJECT}.{DATASET}.tmp_titelive__products

    Example (using defaults):

        # Initial run (creates tables)
        python main.py init-bq

        # Resume from interruption (uses existing tables)
        python main.py init-bq --resume

        # Custom table names
        python main.py init-bq \\
            --tracking-table "project.dataset.custom_tracking" \\
            --processed-eans-table "project.dataset.custom_processed" \\
            --target-table "project.dataset.custom_target"
    """
    logger.info("Executing Mode 1: BigQuery batch processing (optimized)")

    try:
        run_init_bq(
            source_table=source_table,
            tracking_table=tracking_table,
            processed_eans_table=processed_eans_table,
            target_table=target_table,
            batch_size=batch_size,
            project_id=project_id,
            resume=resume,
        )
        logger.info("Mode 1 execution completed successfully")
    except Exception as e:
        logger.error(f"Mode 1 execution failed: {e}")
        raise typer.Exit(code=1) from e


@app.command("init-gcs")
def init_gcs(
    gcs_path: str = typer.Option(
        ...,
        "--gcs-path",
        help="GCS path to file (gs://bucket/path/file.parquet)",
    ),
    target_table: str = typer.Option(
        ...,
        "--target-table",
        help="Target BigQuery table ID (project.dataset.table)",
    ),
    temp_table: str = typer.Option(
        ...,
        "--temp-table",
        help="Temporary BigQuery table ID (project.dataset.table)",
    ),
    project_id: str = typer.Option(
        GCP_PROJECT_ID,
        "--project-id",
        help="GCP project ID",
    ),
) -> None:
    """
    Mode 2: Load GCS file to BigQuery and transform via SQL.

    This mode:
    1. Loads a file from GCS to temporary BigQuery table
    2. Transforms data via SQL query
    3. Inserts transformed data to target table

    Example:

        python main.py init-gcs \\
            --gcs-path "gs://bucket/data/file.parquet" \\
            --temp-table "project.dataset.tmp_gcs_load" \\
            --target-table "project.dataset.tmp_titelive__products"
    """
    logger.info("Executing Mode 2: GCS file bulk load")

    try:
        run_init_gcs(
            gcs_path=gcs_path,
            target_table=target_table,
            temp_table=temp_table,
            project_id=project_id,
        )
        logger.info("Mode 2 execution completed successfully")
    except Exception as e:
        logger.error(f"Mode 2 execution failed: {e}")
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


if __name__ == "__main__":
    app()
