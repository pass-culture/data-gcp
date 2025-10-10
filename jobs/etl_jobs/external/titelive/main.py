"""Main CLI entry point for Titelive ETL pipeline."""

import typer

from config import DEFAULT_SOURCE_TABLE
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
        ...,
        "--tracking-table",
        help="Tracking BigQuery table ID (project.dataset.table)",
    ),
    target_table: str = typer.Option(
        ...,
        "--target-table",
        help="Target BigQuery table ID (project.dataset.table)",
    ),
    batch_size: int = typer.Option(
        DEFAULT_BATCH_SIZE,
        "--batch-size",
        help="Number of EANs to process per batch",
    ),
    project_id: str = typer.Option(
        GCP_PROJECT_ID,
        "--project-id",
        help="GCP project ID",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume from existing tracking table (skip table creation)",
    ),
) -> None:
    """
    Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint.

    This mode:
    1. Extracts EANs from source table
    2. Creates a tracking table to monitor progress
    3. Fetches product data in batches via API
    4. Transforms and loads to target table
    5. Supports resume from interruption

    Example:

        python main.py init-bq \\
            --source-table "project.dataset.source" \\
            --tracking-table "project.dataset.tracking" \\
            --target-table "project.dataset.tmp_titelive__products" \\
            --batch-size 50

        # Resume from interruption:
        python main.py init-bq \\
            --tracking-table "project.dataset.tracking" \\
            --target-table "project.dataset.tmp_titelive__products" \\
            --resume
    """
    logger.info("Executing Mode 1: BigQuery batch processing")

    try:
        run_init_bq(
            source_table=source_table,
            tracking_table=tracking_table,
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
            --min-modified-date "2024-01-01" \\
            --max-modified-date "2024-12-31" \\
            --base "paper" \\
            --target-table "project.dataset.tmp_titelive__products" \\
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
