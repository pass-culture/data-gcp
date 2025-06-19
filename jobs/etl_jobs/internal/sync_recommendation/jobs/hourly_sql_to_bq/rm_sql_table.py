import logging
from datetime import datetime
from typing import Optional

from services.database import BigQueryService, CloudSQLService
from utils.sql_config import SQLTableConfig

logger = logging.getLogger(__name__)


class RemoveSQLTableOrchestrator:
    """Orchestrator for removing processed data from CloudSQL."""

    def __init__(self, database_url: str, project_id: str):
        self.database_url = database_url
        self.cloudsql_service = CloudSQLService(
            connection_params={"database_url": database_url}
        )
        self.bigquery_service = BigQueryService(project_id=project_id)

    def remove_processed_data(
        self,
        table_config: SQLTableConfig,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> None:
        """Remove processed data from CloudSQL.

        Args:
            table_config: SQLTableConfig
            start_time: Start time of the export
            end_time: End time of the export
        """
        logger.info(f"Starting data removal process for {table_config.sql_table_name}")
        query = table_config.get_drop_table_query(start_time, end_time)
        self.cloudsql_service.execute_query(query)

    def get_max_biquery_data_time(
        self,
        table_config: SQLTableConfig,
        days_lag: int = 3,
    ) -> str:
        """Get the maximum time of the table in BigQuery.

        Args:
            table_config: SQLTableConfig
            days_lag: Number of days to lag the current date for partition calculation efficiency

        Returns:
            max_time: Maximum time of the table in BigQuery
        """
        query = f"""
            SELECT max({table_config.time_column})
            FROM {table_config.bigquery_dataset_name}.{table_config.bigquery_table_name}
            WHERE {table_config.partition_field} >= DATE_SUB(CURRENT_DATE, INTERVAL {days_lag} DAY)
        """
        self.bigquery_service.execute_query(query)
        max_time = self.bigquery_service.fetch_one()[0]
        return max_time
