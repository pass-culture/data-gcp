import logging
from datetime import datetime
from typing import List
from utils import ENV_SHORT_NAME, GCP_PROJECT_ID, EXPORT_PATH
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


class Archive:
    def __init__(self, table, config):
        self.table = table
        self.config = config
        self.partition_column = config["partition_column"]
        self.dataset_id = config["dataset_id"]
        self.client = bigquery.Client()
        self.folder = config["folder"]
        self.look_back_months = config["look_back_months"]
        logger.info(f"Initialized Archive for table {self.table} with config: {config}")

    def _get_partition_ids(self, limit: int) -> List[str]:
        """Get partition IDs from INFORMATION_SCHEMA.PARTITIONS with limit, ordered by partition_id ASC."""
        query = f"""
            SELECT DISTINCT partition_id
            FROM `{GCP_PROJECT_ID}.{self.dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name = '{self.table}'
            ORDER BY partition_id ASC
            LIMIT {limit}
        """
        query_job = self.client.query(query)
        return [row["partition_id"] for row in query_job.result()]

    def _parse_partition_date(self, partition_id: str) -> datetime:
        """Parse partition_id string into datetime object."""
        try:
            return datetime.strptime(partition_id, "%Y%m%d")
        except ValueError:
            logger.warning(f"Invalid partition_id format: {partition_id}")
            return None

    def _get_valid_partitions(self, limit: int) -> List[datetime]:
        """Get and validate partition dates."""
        partition_ids = self._get_partition_ids(limit)
        cutoff_date = datetime.now().replace(day=1)  # First day of current month

        valid_partitions = []
        for partition_id in partition_ids:
            partition_date = self._parse_partition_date(partition_id)
            if partition_date and partition_date < cutoff_date:
                valid_partitions.append(partition_date)

        return sorted(valid_partitions)

    def _export_partition(self, partition_date: datetime) -> None:
        """Export a single partition to GCS."""
        partition_str = partition_date.strftime("%Y%m%d")
        gcs_uri = f"{EXPORT_PATH}/{self.folder}/{self.table}/{partition_str}_{self.table}_*.parquet"

        query = f"""
            EXPORT DATA
            OPTIONS (
                uri='{gcs_uri}',
                format='PARQUET',
                overwrite=true
            )
            AS
            SELECT *, {self.partition_column} as partition_date
            FROM `{GCP_PROJECT_ID}.{self.dataset_id}.{self.table}`
            WHERE DATE({self.partition_column}) = '{partition_date.strftime('%Y-%m-%d')}';
        """

        query_job = self.client.query(query)
        query_job.result()
        logger.info(f"Successfully exported partition {partition_str} to {gcs_uri}")

    def _delete_partition(self, partition_date: datetime) -> None:
        """Delete a single partition from the table."""
        query = f"""
            DELETE FROM `{GCP_PROJECT_ID}.{self.dataset_id}.{self.table}`
            WHERE {self.partition_column} = '{partition_date.strftime('%Y-%m-%d')}';
        """

        query_job = self.client.query(query)
        query_job.result()
        logger.info(
            f"Successfully deleted partition {partition_date.strftime('%Y%m%d')} from {self.table}"
        )

    def export_and_delete_partitions(self, limit: int):
        """Main method to export and delete partitions."""
        logger.info(
            f"Starting export and delete process for {self.table} with limit {limit}"
        )

        partitions = self._get_valid_partitions(limit)
        logger.info(f"Found {len(partitions)} partitions to process")

        if not partitions:
            logger.info("No partitions to export")
            return "No partitions to export."

        for partition_date in partitions:
            self._export_partition(partition_date)
            self._delete_partition(partition_date)
