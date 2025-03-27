import logging
import os

from google.cloud import bigquery

from services.database import BigQueryService
from services.storage import StorageService
from utils.sql_config import SQLTableConfig

logger = logging.getLogger(__name__)


class GCSToBQOrchestrator:
    """Orchestrator for importing data from GCS to BigQuery."""

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.bq_service = BigQueryService(project_id=project_id)
        self.storage_service = StorageService(project_id=project_id)

    def import_data(
        self,
        table_config: SQLTableConfig,
        bucket_path: str,
        partition_date_nodash: str,
    ) -> None:
        """Import hourly data from GCS to BigQuery.

        Args:
            table_config: Configuration for the table to process
            bucket_path: GCS bucket path where data is stored
            execution_date: Execution date
        """
        logger.info(
            f"Starting import process for {table_config.bigquery_table_name} at {partition_date_nodash}"
        )

        table_ref = f"{self.project_id}.{table_config.bigquery_dataset_name}.{table_config.bigquery_table_name}${partition_date_nodash}"

        if not self.storage_service.check_files_exists(bucket_path):
            raise Exception(
                f"No data file found at {bucket_path}, skipping BigQuery load"
            )

        try:
            self.bq_service.load_from_gcs(
                table_id=table_ref,
                gcs_path=os.path.join(bucket_path, "*.parquet"),
                schema=table_config.bigquery_schema,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=table_config.partition_field,
                ),
            )
            logger.info(f"Successfully loaded data from {bucket_path} to {table_ref}")

        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {str(e)}")
            raise
