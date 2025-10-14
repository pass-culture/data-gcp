"""Main CLI entry point for Titelive ETL pipeline."""

import typer

from config import (
    DEFAULT_SOURCE_TABLE,
    DEFAULT_TARGET_TABLE,
)
from src.constants import (
    DEFAULT_BATCH_SIZE,
    GCP_PROJECT_ID,
    MAIN_BATCH_SIZE,
    RESULTS_PER_PAGE,
)
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
) -> None:
    """
    Mode 1: Extract EANs from BigQuery and batch process via /ean endpoint.

    New architecture using single destination table with batch_number tracking:
    1. Query destination_table ONCE to get last batch_number
    2. For each batch N:
       - Fetch 20k EANs from source_table using OFFSET pagination
       - Process in sub-batches of 250 EANs (API limit)
       - Handle 3 statuses: processed | deleted_in_titelive | fail
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
