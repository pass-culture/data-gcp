import logging
from datetime import datetime

from google.cloud import bigquery

from config import TableConfig
from utils import ENV_SHORT_NAME, PROJECT_NAME

logger = logging.getLogger(__name__)


def export_table_to_gcs(
    table_name: str,
    table_config: TableConfig,
    bucket_path: str,
    execution_date: datetime,
) -> None:
    """Export a BigQuery table to GCS.

    Args:
        table_name: Name of the table to export
        table_config: Configuration for the table
        bucket_path: Full GCS path for export (e.g. gs://bucket/path)
        execution_date: Execution date for the export
    """
    logger.info(f"Starting export of {table_name} to GCS")

    client = bigquery.Client()

    # Create temporary table with filtered columns
    temp_table_name = f"tmp_{execution_date.strftime('%Y%m%d')}_export_{table_name}"
    query = table_config.get_export_query(PROJECT_NAME)

    try:
        # Execute query to temporary table in tmp_{ENV_SHORT_NAME} dataset
        query_job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                destination=f"{PROJECT_NAME}.tmp_{ENV_SHORT_NAME}.{temp_table_name}",
                write_disposition="WRITE_TRUNCATE",
            ),
        )
        query_job.result()

        # Export to GCS
        dataset_ref = bigquery.DatasetReference(PROJECT_NAME, f"tmp_{ENV_SHORT_NAME}")
        table_ref = dataset_ref.table(temp_table_name)

        destination_uri = f"{bucket_path}/{table_name}-*.csv.gz"

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            job_config=bigquery.ExtractJobConfig(
                compression="GZIP",
                field_delimiter=",",
                print_header=False,
            ),
        )
        extract_job.result()

        logger.info(f"Successfully exported {table_name} to {destination_uri}")

    finally:
        # Clean up temporary table
        client.delete_table(
            f"{PROJECT_NAME}.tmp_{ENV_SHORT_NAME}.{temp_table_name}", not_found_ok=True
        )
