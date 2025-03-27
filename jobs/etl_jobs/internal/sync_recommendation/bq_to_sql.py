import logging
from typing import Annotated

import typer

from jobs.daily_bq_to_sql.export_to_gcs import ExportToGCSOrchestrator
from jobs.daily_bq_to_sql.gcs_to_sql import GCSToSQLOrchestrator
from jobs.daily_bq_to_sql.sql_materialize import SQLMaterializeOrchestrator
from utils.bq_config import EXPORT_TABLES
from utils.constant import PROJECT_NAME, RECOMMENDATION_SQL_INSTANCE
from utils.secret import access_secret_data
from utils.sql_config import MaterializedView
from utils.validate import parse_date, validate_table

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
app = typer.Typer(help="Export/Import data between BigQuery and Cloud SQL")
database_url = access_secret_data(
    PROJECT_NAME,
    f"{RECOMMENDATION_SQL_INSTANCE}_database_url",
)


@app.command()
def bq_to_gcs(
    table_name: Annotated[str, typer.Option(help="Name of the table to export")],
    bucket_path: Annotated[
        str, typer.Option(help="GCS bucket path for temporary storage")
    ],
    date: Annotated[str, typer.Option(help="Date in YYYYMMDD format")],
) -> None:
    """Export a table from BigQuery to GCS."""
    validate_table(table_name, list(EXPORT_TABLES.keys()))
    execution_date = parse_date(date)

    logger.info(f"Starting export for table {table_name}")
    orchestrator = ExportToGCSOrchestrator(project_id=PROJECT_NAME)
    orchestrator.export_table(
        table_config=EXPORT_TABLES[table_name],
        bucket_path=bucket_path,
        execution_date=execution_date,
    )


@app.command()
def gcs_to_cloudsql(
    table_name: Annotated[str, typer.Option(help="Name of the table to import")],
    bucket_path: Annotated[
        str, typer.Option(help="GCS bucket path for temporary storage")
    ],
    date: Annotated[str, typer.Option(help="Date in YYYYMMDD format")],
) -> None:
    """Import a table from GCS to Cloud SQL."""
    validate_table(table_name, list(EXPORT_TABLES.keys()))
    execution_date = parse_date(date)

    logger.info(f"Starting import for table {table_name}")
    orchestrator = GCSToSQLOrchestrator(
        project_id=PROJECT_NAME, database_url=database_url
    )
    orchestrator.import_table(
        table_config=EXPORT_TABLES[table_name],
        bucket_path=bucket_path,
        execution_date=execution_date,
    )


@app.command()
def materialize_cloudsql(
    view_name: Annotated[
        str, typer.Option(help="Name of the materialized view to refresh")
    ],
) -> None:
    """Refresh a materialized view in Cloud SQL."""
    try:
        view = MaterializedView(view_name)
        logger.info(f"Starting materialization for view {view_name}")
        orchestrator = SQLMaterializeOrchestrator(
            project_id=PROJECT_NAME, database_url=database_url
        )
        orchestrator.refresh_view(view)

    except ValueError:
        logger.error(f"Invalid view name: {view_name}", exc_info=True)
        raise typer.BadParameter(f"Invalid view name: {view_name}")


if __name__ == "__main__":
    app()
