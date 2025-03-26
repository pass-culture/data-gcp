import logging
from datetime import datetime
from typing import Annotated, List, Optional

import typer

from jobs.hourly_sql_to_bq.cloudsql_to_gcs import ExportCloudSQLToGCSOrchestrator
from jobs.hourly_sql_to_bq.gcs_to_bq import GCSToBQOrchestrator
from jobs.hourly_sql_to_bq.rm_sql_table import RemoveSQLTableOrchestrator
from utils.constant import PROJECT_NAME, RECOMMENDATION_SQL_INSTANCE
from utils.secret import access_secret_data
from utils.sql_config import EXPORT_TABLES
from utils.validate import parse_date, validate_table

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
app = typer.Typer(help="Export/Import data between BigQuery and Cloud SQL")

database_url = access_secret_data(
    PROJECT_NAME,
    f"{RECOMMENDATION_SQL_INSTANCE}_database_url",
)


def parse_dates(dates: Optional[List[str]] = None) -> Optional[List[datetime]]:
    """Parse a list of date strings to datetime objects."""
    if not dates:
        return None
    return [parse_date(date) for date in dates]


@app.command()
def cloudsql_to_gcs(
    table_name: Annotated[
        str,
        typer.Option(help="Name of the table to process (e.g., past_offer_context)"),
    ],
    bucket_path: Annotated[str, typer.Option(help="GCS bucket path for storage")],
    execution_date: Annotated[
        str, typer.Option(help="Insert execution date in YYYYMMdd format")
    ],
    end_time: Annotated[str, typer.Option(help="End time in YYYY-MM-DD HH format")],
    start_time: Annotated[
        Optional[str],
        typer.Option(
            help="Start time in YYYYMMdd HH format",
        ),
    ] = None,
) -> None:
    """Export data from CloudSQL to GCS."""
    validate_table(table_name, list(EXPORT_TABLES.keys()))
    logger.info(f"Starting export for table {table_name}")
    table_config = EXPORT_TABLES[table_name]

    # Create orchestrator
    orchestrator = ExportCloudSQLToGCSOrchestrator(
        project_id=PROJECT_NAME, database_url=database_url
    )

    if start_time is None:
        start_time = orchestrator._check_min_time(table_config, end_time)

    # Run export process
    orchestrator.export_data(
        table_config=table_config,
        bucket_path=bucket_path,
        execution_date=execution_date,
        start_time=start_time,
        end_time=end_time,
    )


@app.command()
def gcs_to_bq(
    table_name: Annotated[
        str,
        typer.Option(help="Name of the table to process (e.g., past_offer_context)"),
    ],
    bucket_path: Annotated[str, typer.Option(help="GCS bucket path for storage")],
    execution_date: Annotated[
        str, typer.Option(help="Insert execution_date date in YYYYMMdd format")
    ],
) -> None:
    """Import data from GCS to BigQuery."""
    validate_table(table_name, list(EXPORT_TABLES.keys()))
    execution_date_datetime = parse_date(execution_date)

    logger.info(f"Starting import for table {table_name}")

    orchestrator = GCSToBQOrchestrator(project_id=PROJECT_NAME)

    orchestrator.import_data(
        table_config=EXPORT_TABLES[table_name],
        bucket_path=bucket_path,
        partition_date_nodash=execution_date_datetime.strftime("%Y%m%d"),
    )
    logger.info("Successfully imported data to BigQuery")


@app.command()
def remove_cloudsql_data(
    table_name: Annotated[
        str,
        typer.Option(help="Name of the table to process (e.g., past_offer_context)"),
    ],
    start_time: Annotated[
        Optional[str],
        typer.Option(
            help="Start time",
        ),
    ] = None,
    end_time: Annotated[
        Optional[str],
        typer.Option(
            help="End time",
        ),
    ] = None,
) -> None:
    """
    Remove processed data from CloudSQL.
    """
    validate_table(table_name, list(EXPORT_TABLES.keys()))

    # Create orchestrator
    orchestrator = RemoveSQLTableOrchestrator(database_url=database_url)

    table_config = EXPORT_TABLES[table_name]

    if end_time is None:
        end_time = orchestrator.get_max_time(table_config)

    orchestrator.remove_processed_data(
        table_config=table_config, start_time=start_time, end_time=end_time
    )


if __name__ == "__main__":
    app()
