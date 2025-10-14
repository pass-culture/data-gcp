"""BigQuery utilities for table operations and data loading."""

from typing import Literal

import pandas as pd
from google.cloud import bigquery

from src.utils.logging import get_logger

logger = get_logger(__name__)


def get_target_table_schema() -> list[bigquery.SchemaField]:
    """
    Get the standard 3-column schema for Titelive target tables.

    Returns:
        List of BigQuery SchemaField objects
    """
    return [
        bigquery.SchemaField("ean", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("datemodification", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("json_raw", "STRING", mode="REQUIRED"),
    ]


def create_target_table(
    client: bigquery.Client,
    table_id: str,
    drop_if_exists: bool = False,
) -> None:
    """
    Create target table with 3-column schema optimized for Titelive data.

    Schema:
        - ean (STRING): Product EAN, used for clustering
        - datemodification (DATE): Modification date, used for partitioning
        - json_raw (STRING): Full article data as JSON string

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        drop_if_exists: Whether to drop existing table first

    Raises:
        google.cloud.exceptions.GoogleCloudError: If table creation fails
    """
    if drop_if_exists:
        logger.info(f"Dropping table if exists: {table_id}")
        client.delete_table(table_id, not_found_ok=True)

    # Define schema
    schema = get_target_table_schema()

    # Configure table with clustering and partitioning
    table = bigquery.Table(table_id, schema=schema)

    # Partition by datemodification
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="datemodification",
    )

    # Cluster by ean
    table.clustering_fields = ["ean"]

    # Create table
    logger.info(f"Creating table: {table_id}")
    table = client.create_table(table)

    logger.info(f"Created table: {table_id}")
    logger.info(f"  - Clustered by: {table.clustering_fields}")
    logger.info(f"  - Partitioned by: {table.time_partitioning.field}")


def create_processed_eans_table(
    client: bigquery.Client,
    table_id: str,
    drop_if_exists: bool = False,
) -> None:
    """
    Create processed_eans tracking table (append-only).

    This table tracks which EANs have been processed without using UPDATEs.
    Uses LEFT JOIN pattern to find unprocessed EANs, avoiding UPDATE quota limits.

    Schema:
        - ean (STRING): Product EAN, used for clustering
        - processed_at (TIMESTAMP): When the EAN was processed
        - status (STRING): 'success' or 'deleted_in_titelive'

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        drop_if_exists: Whether to drop existing table first

    Raises:
        google.cloud.exceptions.GoogleCloudError: If table creation fails
    """
    if drop_if_exists:
        logger.info(f"Dropping table if exists: {table_id}")
        client.delete_table(table_id, not_found_ok=True)

    # Define schema
    schema = [
        bigquery.SchemaField("ean", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    ]

    # Configure table with clustering
    table = bigquery.Table(table_id, schema=schema)
    table.clustering_fields = ["ean"]

    # Create table
    logger.info(f"Creating processed_eans table: {table_id}")
    table = client.create_table(table)

    logger.info(f"Created processed_eans table: {table_id}")
    logger.info(f"  - Clustered by: {table.clustering_fields}")


def create_tracking_table_from_source(
    client: bigquery.Client,
    tracking_table: str,
    source_table: str,
    drop_if_exists: bool = True,
) -> int:
    """
    Create a tracking table directly from source table using SQL.

    Creates an immutable table containing only EANs to be processed.
    Processing status is tracked separately in processed_eans table.

    Args:
        client: BigQuery client
        tracking_table: Full tracking table ID (project.dataset.table)
        source_table: Full source table ID (project.dataset.table)
        drop_if_exists: Whether to drop existing table first

    Returns:
        Number of EANs inserted into tracking table

    Raises:
        google.cloud.exceptions.GoogleCloudError: If table creation fails
    """
    if drop_if_exists:
        logger.info(f"Dropping tracking table if exists: {tracking_table}")
        client.delete_table(tracking_table, not_found_ok=True)

    # Create tracking table with SQL SELECT (immutable, only EANs)
    query = f"""
        CREATE TABLE `{tracking_table}`
        CLUSTER BY ean
        AS
        SELECT DISTINCT
            JSON_VALUE(jsondata, '$.ean') AS ean
        FROM `{source_table}`
        WHERE JSON_VALUE(jsondata, '$.ean') IS NOT NULL
    """

    logger.info(
        f"Creating tracking table from source (clustered by ean): {source_table}"
    )
    query_job = client.query(query)
    query_job.result()  # Wait for completion

    # Get row count
    table = client.get_table(tracking_table)
    row_count = table.num_rows

    logger.info(f"Created tracking table with {row_count} EANs: {tracking_table}")
    return row_count


def insert_dataframe(
    client: bigquery.Client,
    table_id: str,
    dataframe: pd.DataFrame,
    mode: Literal["append", "replace"] = "append",
    schema: list[bigquery.SchemaField] | None = None,
) -> None:
    """
    Insert a DataFrame into BigQuery table.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        dataframe: Pandas DataFrame to insert
        mode: Write mode - 'append' or 'replace'
        schema: Optional explicit schema (if None, will autodetect for replace mode)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If insert fails
    """
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if mode == "replace"
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
    )

    # Only set schema if explicitly provided
    if schema:
        job_config.schema = schema
    else:
        # For replace mode without explicit schema, use autodetect
        if mode == "replace":
            job_config.autodetect = True

    logger.info(f"Inserting {len(dataframe)} rows to {table_id} (mode={mode})")
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()  # Wait for job to complete

    logger.info(f"Successfully inserted {len(dataframe)} rows to {table_id}")


def get_unprocessed_eans(
    client: bigquery.Client,
    tracking_table: str,
    processed_eans_table: str,
    batch_size: int,
    exclude_eans: set[str] | None = None,
) -> list[str]:
    """
    Fetch unprocessed EANs using LEFT JOIN with processed_eans table.

    Uses LEFT JOIN pattern to find EANs in tracking table that don't exist
    in processed_eans table, avoiding UPDATE quota limits.

    Args:
        client: BigQuery client
        tracking_table: Full tracking table ID (project.dataset.table)
        processed_eans_table: Full processed_eans table ID (project.dataset.table)
        batch_size: Number of EANs to fetch
        exclude_eans: Optional set of EANs to exclude (in-memory buffer)

    Returns:
        List of unprocessed EANs

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    # Build exclude clause for in-memory buffer
    exclude_clause = ""
    if exclude_eans and len(exclude_eans) > 0:
        eans_list = "', '".join(exclude_eans)
        exclude_clause = f"AND t.ean NOT IN ('{eans_list}')"

    query = f"""
        SELECT t.ean
        FROM `{tracking_table}` t
        LEFT JOIN `{processed_eans_table}` p ON t.ean = p.ean
        WHERE p.ean IS NULL
        {exclude_clause}
        LIMIT {batch_size}
    """

    logger.debug(
        f"Fetching up to {batch_size} unprocessed EANs from {tracking_table} "
        f"(excluding {len(exclude_eans) if exclude_eans else 0} in-memory EANs)"
    )
    query_job = client.query(query)
    results = query_job.result()

    eans = [row.ean for row in results]
    logger.info(f"Found {len(eans)} unprocessed EANs")
    return eans


def get_tracking_table_count(
    client: bigquery.Client, tracking_table: str, processed_eans_table: str
) -> int:
    """
    Get count of unprocessed EANs using LEFT JOIN with processed_eans table.

    Args:
        client: BigQuery client
        tracking_table: Full tracking table ID (project.dataset.table)
        processed_eans_table: Full processed_eans table ID (project.dataset.table)

    Returns:
        Number of unprocessed EANs

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT COUNT(*) as total
        FROM `{tracking_table}` t
        LEFT JOIN `{processed_eans_table}` p ON t.ean = p.ean
        WHERE p.ean IS NULL
    """

    query_job = client.query(query)
    result = query_job.result()
    total = next(iter(result)).total

    logger.info(f"Unprocessed EANs in tracking table: {total}")
    return total


def load_gcs_to_bq(
    client: bigquery.Client,
    gcs_path: str,
    table_id: str,
    source_format: Literal["PARQUET", "CSV"] = "PARQUET",
    write_disposition: Literal["TRUNCATE", "APPEND"] = "TRUNCATE",
) -> None:
    """
    Load a file from GCS to BigQuery table.

    Args:
        client: BigQuery client
        gcs_path: GCS path (gs://bucket/path/file)
        table_id: Full destination table ID (project.dataset.table)
        source_format: File format (PARQUET or CSV)
        write_disposition: TRUNCATE or APPEND

    Raises:
        google.cloud.exceptions.GoogleCloudError: If load fails
    """
    job_config = bigquery.LoadJobConfig(
        source_format=getattr(bigquery.SourceFormat, source_format),
        write_disposition=getattr(
            bigquery.WriteDisposition,
            f"WRITE_{write_disposition}",
        ),
        autodetect=True,
    )

    logger.info(f"Loading {gcs_path} to {table_id}")
    load_job = client.load_table_from_uri(gcs_path, table_id, job_config=job_config)
    load_job.result()  # Wait for completion

    destination_table = client.get_table(table_id)
    logger.info(f"Loaded {destination_table.num_rows} rows to {table_id}")


def execute_query(client: bigquery.Client, query: str) -> None:
    """
    Execute a BigQuery SQL query.

    Args:
        client: BigQuery client
        query: SQL query to execute

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    logger.info("Executing BigQuery query")
    logger.debug(f"Query: {query}")

    query_job = client.query(query)
    query_job.result()  # Wait for completion

    logger.info("Query executed successfully")
