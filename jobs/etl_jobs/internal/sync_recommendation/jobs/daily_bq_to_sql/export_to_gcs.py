import logging
from datetime import datetime

from services.database import BigQueryService
from services.storage import StorageService
from utils.bq_config import BQTableConfig
from utils.constant import ENV_SHORT_NAME

logger = logging.getLogger(__name__)


class ExportToGCSOrchestrator:
    """Orchestrator for exporting BigQuery tables to GCS in Parquet format."""

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.bq_service = BigQueryService(project_id=project_id)
        self.storage_service = StorageService(project_id=project_id)

    def export_table(
        self,
        table_config: BQTableConfig,
        bucket_path: str,
        execution_date: datetime,
    ) -> None:
        """Export a BigQuery table to GCS in Parquet format.

        Args:
            table_config: Configuration for the table
            bucket_path: Full GCS path for export (e.g. gs://bucket/path)
            execution_date: Execution date for the export
        """
        logger.info(
            f"Starting export of BigQuery {table_config.bigquery_table_name} to GCS in Parquet format"
        )

        temp_table_name = f"tmp_{execution_date.strftime('%Y%m%d')}_export_{table_config.bigquery_table_name}"
        query = table_config.get_export_query(self.project_id)

        try:
            # Create temporary table
            self.bq_service.execute_query(
                query,
                params={
                    "destination": f"{self.project_id}.tmp_{ENV_SHORT_NAME}.{temp_table_name}",
                    "write_disposition": "WRITE_TRUNCATE",
                },
            )

            # Export to GCS
            source_ref = f"{self.project_id}.tmp_{ENV_SHORT_NAME}.{temp_table_name}"
            destination_uri = (
                f"{bucket_path}/{table_config.bigquery_table_name}-*.parquet"
            )

            self.bq_service.export_data(source_ref, destination_uri)
            logger.info(f"Successfully exported to {destination_uri}")

        finally:
            # Clean up temporary table
            self.bq_service.execute_query(
                f"DROP TABLE IF EXISTS {self.project_id}.tmp_{ENV_SHORT_NAME}.{temp_table_name}"
            )
