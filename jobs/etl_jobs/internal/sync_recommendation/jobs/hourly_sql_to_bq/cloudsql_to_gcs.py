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
    """Orchestrator for exporting data from CloudSQL to GCS."""

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
        start_time: datetime,
        end_time: datetime,
    ) -> List[str]:
        """Export hourly data from CloudSQL to GCS.

        Args:
            table_name: Name of the table to process
            table_config: Configuration for the table
            bucket_path: GCS bucket path for storage
            execution_date: Execution date format
            start_time: Start time of the export
            end_time: End time of the export

        Returns:
            List of GCS paths where data was exported
        """
        logger.info(
            f"Starting export process for {table_config.sql_table_name} at {start_time.strftime('%Y-%m-%d')} hour {start_time.hour}"
        )

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
            # Clean up temporary directory
            if temp_dir.exists():
                for file in temp_dir.glob("*"):
                    file.unlink()
                temp_dir.rmdir()

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
        parquet_path = temp_dir / "*.parquet"
        query = table_config.get_extract_query(
            start_time, end_time, execution_date=execution_date
        )

        # Export directly to Parquet using DuckDB
        self.duck_service.cloudsql_to_parquet(
            query=query, destination_path=str(parquet_path)
        )

        # Upload to GCS
        gcs_dir = f"{bucket_path}"
        gcs_paths = self.storage_service.upload_files(
            source_paths=[str(parquet_path)], destination_dir=gcs_dir
        )

        logger.info(f"Successfully exported data to {gcs_paths[0]}")
        return gcs_paths

    def _check_min_time(
        self,
        table_config: SQLTableConfig,
        execution_date: datetime,
    ) -> str:
        """Check for missed data in previous hours."""
        date_str = execution_date.strftime("%Y-%m-%d")

        query = f"""
            SELECT min({table_config.time_column})
            FROM {table_config.sql_table_name}
            WHERE {table_config.partition_field} <= '{date_str}'
        """
        self.cloudsql_service.execute_query(query)
        min_time = self.cloudsql_service.fetch_one()[0]
        return min_time
