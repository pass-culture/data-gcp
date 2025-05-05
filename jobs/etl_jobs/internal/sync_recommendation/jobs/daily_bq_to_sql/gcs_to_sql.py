import logging
import os
import time
from datetime import datetime
from typing import List

from services.database import CloudSQLService, DuckDBService
from services.storage import StorageService
from utils.bq_config import BQTableConfig

logger = logging.getLogger(__name__)


class GCSToSQLOrchestrator:
    """Orchestrator for importing data from GCS to CloudSQL using DuckDB as an intermediary."""

    def __init__(self, project_id: str, database_url: str):
        self.project_id = project_id
        self.database_url = database_url
        self.cloudsql_service = CloudSQLService(
            connection_params={"database_url": database_url}
        )
        self.storage_service = StorageService(project_id=project_id)
        self.duck_service = DuckDBService()

    def import_table(
        self,
        table_config: BQTableConfig,
        bucket_path: str,
        execution_date: datetime,
    ) -> None:
        """Import a table from GCS Parquet files to Cloud SQL using DuckDB as an intermediary.

        Args:
            table_config: Configuration for the table
            bucket_path: Full GCS path where the export is stored
            execution_date: Execution date for the import
        """
        logger.info(
            f"Starting import of BigQuery {table_config.bigquery_table_name} table to Cloud SQL {table_config.cloud_sql_table_name} table using DuckDB"
        )
        start_time = time.time()
        parquet_files = []

        try:
            # Download Parquet files from GCS
            parquet_files = self._download_parquet_files(
                bucket_path, table_config.bigquery_table_name
            )

            # Create a temporary DuckDB database
            duck_db_path = f"/tmp/{table_config.bigquery_table_name}_{execution_date.strftime('%Y%m%d')}.duckdb"
            self.duck_service.database_path = duck_db_path

            try:
                # Create a view of all Parquet files
                parquet_files_str = ", ".join([f"'{file}'" for file in parquet_files])
                logger.info("Creating DuckDB view from Parquet files")
                self.duck_service.execute_query(
                    f"CREATE VIEW parquet_data AS SELECT * FROM parquet_scan([{parquet_files_str}])"
                )

                # Get column definitions for PostgreSQL table
                columns_sql = table_config.column_definitions

                # Attach PostgreSQL database
                logger.info("Attaching PostgreSQL database")
                self.duck_service.execute_query(
                    f"ATTACH '{self.database_url}' AS pg_db (TYPE postgres)"
                )

                # Drop existing table in PostgreSQL if it exists
                logger.info(
                    f"Dropping existing table {table_config.cloud_sql_table_name} if it exists"
                )
                self.duck_service.execute_query(
                    f"DROP TABLE IF EXISTS pg_db.{table_config.cloud_sql_table_name}"
                )

                # Create table in PostgreSQL
                logger.info(
                    f"Creating table {table_config.cloud_sql_table_name} in PostgreSQL"
                )
                create_table_sql = f"CREATE TABLE pg_db.{table_config.cloud_sql_table_name} ({columns_sql})"
                self.duck_service.execute_query(create_table_sql)

                # Create view with proper column conversions
                logger.info("Creating view with column conversions")
                self.duck_service.execute_query(
                    f"CREATE VIEW parquet_data_wkt AS SELECT {table_config.duckdb_select_columns} FROM parquet_data"
                )

                # Copy data from Parquet to PostgreSQL using the WKT view
                logger.info(
                    f"Copying data from Parquet to PostgreSQL table {table_config.cloud_sql_table_name}"
                )
                copy_start_time = time.time()
                self.duck_service.execute_query(
                    f"INSERT INTO pg_db.{table_config.cloud_sql_table_name} SELECT * FROM parquet_data_wkt"
                )

                copy_elapsed_time = time.time() - copy_start_time

                # Get row count
                self.duck_service.execute_query(
                    f"SELECT COUNT(*) FROM pg_db.{table_config.cloud_sql_table_name}"
                )
                total_rows = self.duck_service.fetch_one()[0]

                logger.info(
                    f"Successfully copied {total_rows} rows to PostgreSQL table {table_config.cloud_sql_table_name} in {copy_elapsed_time:.2f} seconds"
                )

                # Detach PostgreSQL database
                self.duck_service.execute_query("DETACH pg_db")

            finally:
                # Close DuckDB connection
                self.duck_service.close()
                # Remove temporary DuckDB database
                if os.path.exists(duck_db_path):
                    os.remove(duck_db_path)

            elapsed_time = time.time() - start_time
            logger.info(
                f"Successfully imported BigQuery {table_config.bigquery_table_name} table to Cloud SQL {table_config.cloud_sql_table_name} table in {elapsed_time:.2f} seconds"
            )

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(
                f"Failed to import BigQuery {table_config.bigquery_table_name} table to Cloud SQL {table_config.cloud_sql_table_name} table after {elapsed_time:.2f} seconds: {str(e)}"
            )
            raise
        finally:
            # Clean up downloaded Parquet files
            self.storage_service.cleanup_files(parquet_files)

    def _download_parquet_files(self, bucket_path: str, table_name: str) -> List[str]:
        """Download Parquet files from GCS and return their local paths.

        Args:
            bucket_path: GCS bucket path (gs://bucket-name/path)
            table_name: Name of the table to import

        Returns:
            List of local file paths to downloaded Parquet files
        """
        # Create temporary directory for downloaded files
        temp_dir = "/tmp/parquet_imports"
        os.makedirs(temp_dir, exist_ok=True)

        # Download files using StorageService
        prefix = f"{table_name}-"
        parquet_files = self.storage_service.download_files(
            bucket_path=bucket_path, prefix=prefix, destination_dir=temp_dir
        )
        if not parquet_files:
            raise ValueError(
                f"No Parquet files found for table {table_name} at {bucket_path}/{prefix}"
            )

        logger.info(f"Found {len(parquet_files)} Parquet files for table {table_name}")
        return parquet_files
