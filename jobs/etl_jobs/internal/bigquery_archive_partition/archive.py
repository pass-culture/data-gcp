import logging
from datetime import datetime, timedelta
from typing import List

from pydantic import BaseModel
from utils import ENV_SHORT_NAME, GCP_PROJECT_ID, EXPORT_PATH
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


class JobConfig(BaseModel):
    table_id: str
    dataset_id: str
    partition_column: str
    look_back_days: int
    folder: str
    archive: bool


class Archive:
    def __init__(self, config: JobConfig):
        self.config = config
        self.table_id = config.table_id
        self.partition_column = config.partition_column
        self.dataset_id = config.dataset_id
        self.client = bigquery.Client()
        self.folder = config.folder
        self.archive = config.archive
        self.look_back_days = config.look_back_days
        self.cutoff_date = (
            datetime.now() - timedelta(days=self.look_back_days)
        ).replace(day=1)  # First day of current month - look_back_days

        logger.info(
            f"Initialized Archive for table {self.table_id} with config: {config}, cutoff_date: {self.cutoff_date}"
        )

    def _get_partition_ids(self, limit: int) -> List[str]:
        """Get partition IDs from INFORMATION_SCHEMA.PARTITIONS with limit, ordered by partition_id ASC."""
        query = f"""
            SELECT DISTINCT partition_id
            FROM `{GCP_PROJECT_ID}.{self.dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name = '{self.table_id}'
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

        valid_partitions = []
        for partition_id in partition_ids:
            partition_date = self._parse_partition_date(partition_id)
            if partition_date and partition_date < self.cutoff_date:
                valid_partitions.append(partition_date)

        return sorted(valid_partitions)

    def _export_partition(self, partition_date: datetime) -> None:
        """Export a single partition to GCS."""
        partition_str = partition_date.strftime("%Y%m%d")
        gcs_uri = f"{EXPORT_PATH}/{self.folder}/{self.table_id}/{partition_str}_{self.table_id}_*.parquet"

        query = f"""
            EXPORT DATA
            OPTIONS (
                uri='{gcs_uri}',
                format='PARQUET',
                overwrite=true
            )
            AS
            SELECT *, {self.partition_column} as partition_date
            FROM `{GCP_PROJECT_ID}.{self.dataset_id}.{self.table_id}`
            WHERE DATE({self.partition_column}) = '{partition_date.strftime('%Y-%m-%d')}';
        """

        query_job = self.client.query(query)
        query_job.result()
        logger.info(f"Successfully exported partition {partition_str} to {gcs_uri}")

    def _delete_partition(self, partition_date: datetime) -> None:
        """Delete a single partition from the table."""
        query = f"""
            DELETE FROM `{GCP_PROJECT_ID}.{self.dataset_id}.{self.table_id}`
            WHERE {self.partition_column} = '{partition_date.strftime('%Y-%m-%d')}';
        """

        query_job = self.client.query(query)
        query_job.result()
        logger.info(
            f"Successfully deleted partition {partition_date.strftime('%Y%m%d')} from {self.table_id}"
        )

    def archive_partitions(self, limit: int):
        """Main method to export and delete partitions."""
        logger.info(
            f"Starting export and delete process for {self.table_id} with limit {limit}"
        )

        partitions = self._get_valid_partitions(limit)
        logger.info(f"Found {len(partitions)} partitions to process")

        if not partitions:
            logger.info("No partitions to export")
            return "No partitions to export."

        for partition_date in partitions:
            if self.archive:
                logger.info(
                    f"Exporting partition {partition_date.strftime('%Y%m%d')} to {EXPORT_PATH}/{self.folder}/{self.table_id}/{partition_date.strftime('%Y%m%d')}_{self.table_id}_*.parquet"
                )
                self._export_partition(partition_date)
            logger.info(
                f"Deleting partition {partition_date.strftime('%Y%m%d')} from {self.table_id}"
            )
            self._delete_partition(partition_date)
