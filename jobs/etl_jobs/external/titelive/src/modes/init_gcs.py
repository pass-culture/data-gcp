"""Mode 2: Load GCS file to BigQuery temp table and transform via SQL."""

from google.cloud import bigquery

from src.constants import GCP_PROJECT_ID
from src.transformers.sql_transform import get_transform_query
from src.utils.bigquery import execute_query, load_gcs_to_bq
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_init_gcs(
    gcs_path: str,
    target_table: str,
    temp_table: str,
    project_id: str = GCP_PROJECT_ID,
) -> None:
    """
    Execute Mode 2: GCS file bulk load via BigQuery.

    Workflow:
    1. Load GCS file to temp BigQuery table
    2. Transform data via SQL query
    3. Insert transformed data to target table
    4. (Optional) Clean up temp table

    Args:
        gcs_path: GCS path to file (gs://bucket/path/file.parquet)
        target_table: Full table ID for target (project.dataset.table)
        temp_table: Full table ID for temp table (project.dataset.temp)
        project_id: GCP project ID

    Raises:
        Exception: If any step fails
    """
    logger.info("Starting Mode 2: GCS file bulk load")
    logger.info(f"GCS Path: {gcs_path}")
    logger.info(f"Temp Table: {temp_table}")
    logger.info(f"Target Table: {target_table}")

    # Initialize BigQuery client
    bq_client = bigquery.Client(project=project_id)

    # Step 1: Load GCS file to temp table
    logger.info("Step 1: Loading GCS file to temp BigQuery table")

    # Determine file format from extension
    source_format = "PARQUET" if gcs_path.endswith(".parquet") else "CSV"

    load_gcs_to_bq(
        client=bq_client,
        gcs_path=gcs_path,
        table_id=temp_table,
        source_format=source_format,
        write_disposition="TRUNCATE",
    )

    # Step 2: Transform data via SQL
    logger.info("Step 2: Transforming data via SQL")

    transform_query = get_transform_query(
        source_table=temp_table,
        target_table=target_table,
    )

    logger.debug(f"Transform query: {transform_query}")

    execute_query(bq_client, transform_query)

    # Step 3: Verify results
    logger.info("Step 3: Verifying results")

    target_table_obj = bq_client.get_table(target_table)
    logger.info(f"Target table now has {target_table_obj.num_rows} rows")

    # Optional: Clean up temp table (commented out for now)
    # logger.info("Step 4: Cleaning up temp table")
    # bq_client.delete_table(temp_table, not_found_ok=True)
    # logger.info(f"Deleted temp table: {temp_table}")

    logger.info("Mode 2 complete.")
