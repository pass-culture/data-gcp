import logging
from datetime import datetime

import typer
from typing_extensions import Annotated

from config import (
    TABLES,
    MaterializedView,
)
from jobs.export import export_table_to_gcs
from jobs.import_sql import import_table_to_sql
from jobs.materialize import refresh_materialized_view

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Export/Import data between BigQuery and Cloud SQL")


def validate_table(table_name: str) -> None:
    """Validate that the table exists in configuration."""
    if table_name not in TABLES:
        raise typer.BadParameter(f"Table {table_name} not found in configuration")


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object."""
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        raise typer.BadParameter("Date must be in YYYYMMDD format")


@app.command()
def export_gcs(
    table_name: Annotated[str, typer.Option(help="Name of the table to export")],
    bucket_path: Annotated[
        str, typer.Option(help="GCS bucket path for temporary storage")
    ],
    date: Annotated[str, typer.Option(help="Date in YYYYMMDD format")],
) -> None:
    """Export a table from BigQuery to GCS."""
    validate_table(table_name)
    execution_date = parse_date(date)

    logger.info(f"Starting export for table {table_name}")
    export_table_to_gcs(
        table_name=table_name,
        table_config=TABLES[table_name],
        bucket_path=bucket_path,
        execution_date=execution_date,
    )


@app.command()
def import_to_gcloud(
    table_name: Annotated[str, typer.Option(help="Name of the table to import")],
    bucket_path: Annotated[
        str, typer.Option(help="GCS bucket path for temporary storage")
    ],
    date: Annotated[str, typer.Option(help="Date in YYYYMMDD format")],
) -> None:
    """Import a table from GCS to Cloud SQL."""
    validate_table(table_name)
    execution_date = parse_date(date)

    logger.info(f"Starting import for table {table_name}")
    import_table_to_sql(
        table_name=table_name,
        table_config=TABLES[table_name],
        bucket_path=bucket_path,
        execution_date=execution_date,
    )


@app.command()
def materialize_gcloud(
    view_name: Annotated[
        str, typer.Option(help="Name of the materialized view to refresh")
    ],
) -> None:
    """Refresh a materialized view in Cloud SQL."""
    try:
        view = MaterializedView(view_name)
        refresh_materialized_view(view)
    except ValueError:
        raise typer.BadParameter(f"Invalid view name: {view_name}")


if __name__ == "__main__":
    app()
