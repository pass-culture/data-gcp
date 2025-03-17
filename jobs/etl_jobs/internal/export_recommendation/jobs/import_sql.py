import logging
import time
from datetime import datetime

from google.cloud import sql_v1beta4
from google.cloud.sql.connector import Connector
from psycopg2.extensions import connection

from config import CLOUD_SQL_IMPORT_CONFIG, TableConfig
from utils import (
    PROJECT_NAME,
    RECOMMENDATION_SQL_INSTANCE,
    access_secret_data,
)

logger = logging.getLogger(__name__)


def get_db_connection() -> connection:
    """Create a database connection with retries."""
    database_url = access_secret_data(
        PROJECT_NAME,
        f"{RECOMMENDATION_SQL_INSTANCE}_database_url",
    )

    connector = Connector()
    conn: connection = connector.connect(
        database_url,
        "pg8000",
        user_agent="cloud-sql-python",
        enable_iam_auth=True,
    )
    return conn


def import_table_to_sql(
    table_name: str,
    table_config: TableConfig,
    bucket_path: str,
    execution_date: datetime,
) -> None:
    """Import a table from GCS to Cloud SQL.

    Args:
        table_name: Name of the table to import
        table_config: Configuration for the table
        bucket_path: Full GCS path where the export is stored
        execution_date: Execution date for the import
    """
    logger.info(f"Starting import of {table_name} to Cloud SQL")

    try:
        # Set up import URI
        import_uri = f"{bucket_path}/{table_name}-*.csv.gz"

        # Get Cloud SQL instance name
        instance_name = access_secret_data(
            PROJECT_NAME,
            f"{RECOMMENDATION_SQL_INSTANCE}_database_instance_name",
        )

        # Configure import job
        import_job_config = {
            **CLOUD_SQL_IMPORT_CONFIG,
            "uri": import_uri,
            "database": access_secret_data(
                PROJECT_NAME,
                f"{RECOMMENDATION_SQL_INSTANCE}_database_name",
            ),
        }

        # Set table-specific options
        import_job_config["csvImportOptions"]["table"] = table_name
        import_job_config["csvImportOptions"]["columns"] = table_config.columns

        # Initialize Cloud SQL Admin API client
        client = sql_v1beta4.CloudSqlClient()

        # Create import request
        import_request = sql_v1beta4.ImportInstanceRequest(
            project=PROJECT_NAME,
            instance=instance_name,
            import_context=import_job_config,
        )

        # Execute import operation
        operation = client.import_instance(request=import_request)

        # Wait for the operation to complete
        while not operation.done():
            logger.info(f"Import operation for {table_name} in progress...")
            time.sleep(10)
            operation.reload()

        if operation.error:
            raise Exception(f"Import failed: {operation.error}")

        logger.info(f"Successfully imported {table_name} to Cloud SQL")

    except Exception as e:
        logger.error(f"Failed to import {table_name} to Cloud SQL: {str(e)}")
        raise
