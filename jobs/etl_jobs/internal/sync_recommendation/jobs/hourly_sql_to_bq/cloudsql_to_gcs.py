import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from services.database import CloudSQLService, DuckDBService
from services.storage import StorageService
from utils.sql_config import SQLTableConfig

logger = logging.getLogger(__name__)


class ExportCloudSQLToGCSOrchestrator:
    """Orchestrator for exporting data from CloudSQL to GCS using DuckDB as an intermediary."""

    def __init__(self, project_id: str, database_url: str):
        self.project_id = project_id
        self.database_url = database_url
        self.cloudsql_service = CloudSQLService(
            connection_params={"database_url": database_url}
        )
        self.storage_service = StorageService(project_id=project_id)
        self.duck_service = DuckDBService()

    def export_data(
        self,
        table_config: SQLTableConfig,
        bucket_path: str,
        execution_date: datetime,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[str]:
        """Export hourly data from CloudSQL to GCS.

        Args:
            table_name: Name of the table to process
            table_config: Configuration for the table
            bucket_path: GCS bucket path for storage
            execution_date: Execution date format
            start_time: Start time of the export, if None, it will be the minimum time of the table in CloudSQL
            end_time: End time of the export

        Returns:
            List of GCS paths where data was exported
        """

        logger.info(f"Starting export process for {table_config.sql_table_name}")

        # Create temporary directory
        temp_dir = (
            Path("/tmp")
            / "cloudsql_export"
            / f"{table_config.sql_table_name}"
            / f"{uuid.uuid4()}"
        )
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Export from CloudSQL to GCS
            gcs_paths = self._export_to_gcs(
                table_config,
                bucket_path,
                execution_date,
                start_time,
                end_time,
                temp_dir,
            )

            if not gcs_paths:
                logger.info("No data to export")
                return []

            logger.info(f"Successfully exported data to {len(gcs_paths)} GCS paths")
            return gcs_paths

        finally:
            if temp_dir.exists():
                for file in temp_dir.glob("*"):
                    file.unlink()
                temp_dir.rmdir()

            self.cloudsql_service.close()

    def _export_to_gcs(
        self,
        table_config: SQLTableConfig,
        bucket_path: str,
        execution_date: datetime,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        temp_dir: Path,
    ) -> List[str]:
        """Export data from CloudSQL to GCS using DuckDB."""
        parquet_path = temp_dir / f"{table_config.sql_table_name}.parquet"
        database_name = "pg_db.public"
        query = table_config.get_extract_query(
            start_time,
            end_time,
            execution_date=execution_date,
            database_name=database_name,
        )

        conn = None
        try:
            conn = self.duck_service.setup_connection()
            conn.execute(f"ATTACH '{self.database_url}' AS pg_db (TYPE postgres)")
            logger.info(f"Executing query: {query}")
            conn.execute(f"""
                COPY (
                    {query}
                ) TO '{str(parquet_path)}'
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)
            logger.info(f"Successfully exported data to {str(parquet_path)}")

            # Upload to GCS
            gcs_dir = f"{bucket_path}"
            gcs_paths = self.storage_service.upload_files(
                source_paths=[str(parquet_path)], destination_dir=gcs_dir
            )

            logger.info(f"Successfully exported data to {gcs_paths[0]}")
            return gcs_paths

        except Exception as e:
            logger.error(f"Error exporting data to GCS: {str(e)}")
            raise
        finally:
            try:
                if conn:
                    conn.execute("DETACH pg_db")
            except Exception as e:
                logger.warning(f"Error detaching database: {str(e)}")
