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


def get_destination_table_schema() -> list[bigquery.SchemaField]:
    """
    Get the schema for Titelive destination table with batch tracking.

    Schema includes:
        - ean: Product EAN
        - status: Processing status (processed|deleted_in_titelive|fail)
        - processed_at: Processing timestamp
        - datemodification: Modification date (NULL for deleted/failed)
        - json_raw: Full article JSON (NULL for deleted/failed)
        - batch_number: Batch number for progress tracking

    Returns:
        List of BigQuery SchemaField objects
    """
    return [
        bigquery.SchemaField("ean", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("datemodification", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("json_raw", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("batch_number", "INTEGER", mode="REQUIRED"),
    ]


def create_destination_table(
    client: bigquery.Client,
    table_id: str,
    drop_if_exists: bool = False,
) -> None:
    """
    Create destination table with batch tracking schema.

    Schema:
        - ean (STRING): Product EAN, used for clustering
        - status (STRING): Processing status (processed|deleted_in_titelive|fail)
        - processed_at (TIMESTAMP): Processing timestamp
        - datemodification (DATE): Modification date (NULL for deleted/failed)
        - json_raw (STRING): Full article data as JSON (NULL for deleted/failed)
        - batch_number (INTEGER): Batch number for progress tracking

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
    schema = get_destination_table_schema()

    # Configure table with clustering
    table = bigquery.Table(table_id, schema=schema)
    table.clustering_fields = ["ean"]

    # Create table
    logger.info(f"Creating destination table: {table_id}")
    table = client.create_table(table)

    logger.info(f"Created destination table: {table_id}")
    logger.info(f"  - Clustered by: {table.clustering_fields}")


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

    # Configure table with clustering (no partitioning to avoid quota issues)
    table = bigquery.Table(table_id, schema=schema)

    # Cluster by ean
    table.clustering_fields = ["ean"]

    # Create table
    logger.info(f"Creating table: {table_id}")
    table = client.create_table(table)

    logger.info(f"Created table: {table_id}")
    logger.info(f"  - Clustered by: {table.clustering_fields}")


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
        WHERE NOT EXISTS (
            SELECT 1 FROM `{processed_eans_table}` p WHERE p.ean = t.ean
        )
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
        WHERE NOT EXISTS (
            SELECT 1 FROM `{processed_eans_table}` p WHERE p.ean = t.ean
        )
    """

    query_job = client.query(query)
    result = query_job.result()
    total = next(iter(result)).total

    logger.info(f"Unprocessed EANs in tracking table: {total}")
    return total


def get_last_batch_number(client: bigquery.Client, destination_table: str) -> int:
    """
    Get the last batch number from destination table.

    This function queries the destination table ONCE at the start to determine
    which batch to resume from. Returns -1 if table is empty (start at batch 0).

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)

    Returns:
        Last batch number, or -1 if table is empty

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT COALESCE(MAX(batch_number), -1) as last_batch
        FROM `{destination_table}`
    """

    logger.info(f"Querying last batch number from {destination_table}")
    query_job = client.query(query)
    result = query_job.result()
    last_batch = next(iter(result)).last_batch

    logger.info(f"Last batch number: {last_batch} (next batch: {last_batch + 1})")
    return last_batch


def fetch_batch_eans(
    client: bigquery.Client,
    source_table: str,
    batch_number: int,
    batch_size: int = 20_000,
    skip_already_processed_table: str | None = None,
    skip_count: int = 0,
) -> list[str]:
    """
    Fetch EANs for a specific batch using OFFSET pagination.

    Uses OFFSET + LIMIT + ORDER BY to deterministically fetch batch N.
    OFFSET = skip_count + (batch_number * batch_size)

    When skip_already_processed_table is provided, uses ORDER BY with CASE WHEN
    to sort already-processed EANs first, then applies OFFSET to skip them.

    Args:
        client: BigQuery client
        source_table: Full source table ID (project.dataset.table)
        batch_number: Batch number to fetch (0-indexed)
        batch_size: Number of EANs per batch (default 20,000)
        skip_already_processed_table: Optional table containing already-processed EANs
        skip_count: Number of already-processed EANs \
            to skip (used with skip_already_processed_table)

    Returns:
        List of EANs for this batch (up to batch_size)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    offset = skip_count + (batch_number * batch_size)

    # Build ORDER BY clause
    if skip_already_processed_table:
        # Sort already-processed EANs first, then unprocessed EANs
        order_by_clause = f"""
            CASE
                WHEN ean IN (SELECT ean FROM `{skip_already_processed_table}`)
                THEN 0
                ELSE 1
            END,
            ean
        """
        logger.info(
            "Using skip logic: already-processed EANs from "
            f"{skip_already_processed_table} will be sorted first and skipped"
        )
    else:
        order_by_clause = "ean"

    query = f"""
        SELECT DISTINCT JSON_VALUE(jsondata, '$.ean') AS ean
        FROM `{source_table}`
        WHERE JSON_VALUE(jsondata, '$.ean') IS NOT NULL
        ORDER BY {order_by_clause}
        LIMIT {batch_size}
        OFFSET {offset}
    """

    logger.info(
        f"Fetching batch {batch_number}: OFFSET {offset} "
        f"(skip_count={skip_count}, batch_offset={batch_number * batch_size}), "
        f"LIMIT {batch_size}"
    )
    query_job = client.query(query)
    results = query_job.result()

    eans = [row.ean for row in results]
    logger.info(f"Fetched {len(eans)} EANs for batch {batch_number}")
    return eans


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
