"""BigQuery utilities for table operations and data loading."""

from typing import Literal

import pandas as pd
from google.cloud import bigquery
from workflows.titelive.config import (
    PRODUCT_MEDIATION_TABLE,
    PRODUCT_TABLE,
    TITELIVE_PROVIDER_IDS,
)
from workflows.titelive.logging_utils import get_logger

logger = get_logger(__name__)


def get_destination_table_schema() -> list[bigquery.SchemaField]:
    """
    Get the schema for Titelive destination table with batch tracking.

    Schema includes:
        - ean: Product EAN
        - subcategoryid: Subcategory ID for API base routing (NULL if unknown)
        - status: Processing status (processed|deleted_in_titelive|fail)
        - processed_at: Processing timestamp
        - json_raw: Full article JSON (NULL for deleted/failed)
        - batch_number: Batch number for progress tracking
        - images_download_status: Image download status (processed|failed|NULL)
        - images_download_processed_at: Image download timestamp (NULL if not attempted)
        - recto_image_uuid: UUID of recto image in GCS (NULL if not downloaded)
        - verso_image_uuid: UUID of verso image in GCS (NULL if not downloaded)

    Returns:
        List of BigQuery SchemaField objects
    """
    return [
        bigquery.SchemaField("ean", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("subcategoryid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("json_raw", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("batch_number", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("images_download_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "images_download_processed_at", "TIMESTAMP", mode="NULLABLE"
        ),
        bigquery.SchemaField("recto_image_uuid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("verso_image_uuid", "STRING", mode="NULLABLE"),
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
        - subcategoryid (STRING): Subcategory ID for API base routing (NULL if unknown)
        - status (STRING): Processing status (processed|deleted_in_titelive|fail)
        - processed_at (TIMESTAMP): Processing timestamp
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
        clustering_fields=["ean"],
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
) -> list[tuple[str, str]]:
    """
    Fetch EANs with subcategoryid for a specific batch using OFFSET pagination.

    Uses OFFSET + LIMIT + ORDER BY to deterministically fetch batch N.
    OFFSET = batch_number * batch_size

    Args:
        client: BigQuery client
        source_table: Full source table ID (project.dataset.table)
        batch_number: Batch number to fetch (0-indexed)
        batch_size: Number of EANs per batch (default 20,000)

    Returns:
        List of tuples (ean, subcategoryid) for this batch (up to batch_size)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    offset = batch_number * batch_size

    query = f"""
        SELECT DISTINCT
            ean,
            subcategoryid
        FROM `{source_table}`
        WHERE ean IS NOT NULL
        ORDER BY ean
        LIMIT {batch_size}
        OFFSET {offset}
    """

    logger.info(f"Fetching batch {batch_number}: OFFSET {offset}, LIMIT {batch_size}")
    query_job = client.query(query)
    results = query_job.result()

    ean_pairs = [(row.ean, row.subcategoryid) for row in results]
    logger.info(f"Fetched {len(ean_pairs)} EANs for batch {batch_number}")
    return ean_pairs


def count_failed_eans(client: bigquery.Client, destination_table: str) -> int:
    """
    Count the number of EANs with status='failed' in destination table.

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)

    Returns:
        Number of failed EANs

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT COUNT(*) as total
        FROM `{destination_table}`
        WHERE status = 'failed'
    """

    logger.info(f"Counting failed EANs in {destination_table}")
    query_job = client.query(query)
    result = query_job.result()
    total = next(iter(result)).total

    logger.info(f"Found {total} failed EANs in destination table")
    return total


def get_last_sync_date(
    client: bigquery.Client,
    provider_event_table: str,
    base: str,
    provider_ids: list[int] = TITELIVE_PROVIDER_IDS,
) -> str | None:
    """
    Get the last sync date for a given base from provider event table.

    Queries the provider event table to find the most recent SyncEnd event
    across all specified Titelive-related providers for the given base.

    Args:
        client: BigQuery client
        provider_event_table: Full provider event table ID (project.dataset.table)
        base: Product base (e.g., "paper" or "music")
        provider_ids: List of provider IDs to query (defaults to TITELIVE_PROVIDER_IDS)

    Returns:
        Last sync date as string (YYYY-MM-DD format), or None if no sync found

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    # Convert provider IDs list to SQL IN clause
    provider_ids_str = ",".join(map(str, provider_ids))

    query = f"""
        SELECT
            MAX(date) AS last_sync_date
        FROM `{provider_event_table}`
        WHERE
            CAST(providerId AS INT64) IN ({provider_ids_str})
            AND payload = '{base}'
            AND type = 'SyncEnd'
    """

    logger.info(
        f"Querying last sync date for base={base}, " f"providers={provider_ids_str}"
    )
    query_job = client.query(query)
    result = query_job.result()
    row = next(iter(result), None)

    if row and row.last_sync_date:
        last_sync_date = row.last_sync_date.strftime("%Y-%m-%d")
        logger.info(f"Last sync date for {base}: {last_sync_date}")
        return last_sync_date
    else:
        logger.warning(f"No previous sync found for {base}")
        return None


def fetch_failed_eans(
    client: bigquery.Client,
    destination_table: str,
    batch_size: int = 20_000,
) -> list[tuple[str, str]]:
    """
    Fetch EANs with subcategoryid with status='failed' from destination table.

    Used for reprocessing failed EANs from previous runs.

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)
        batch_size: Number of failed EANs to fetch (default 20,000)

    Returns:
        List of tuples (ean, subcategoryid) for failed EANs (up to batch_size)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT ean, subcategoryid
        FROM `{destination_table}`
        WHERE status = 'failed'
        ORDER BY ean
        LIMIT {batch_size}
    """

    logger.info(f"Fetching up to {batch_size} failed EANs from {destination_table}")
    query_job = client.query(query)
    results = query_job.result()

    ean_pairs = [(row.ean, row.subcategoryid) for row in results]
    logger.info(f"Fetched {len(ean_pairs)} failed EANs for reprocessing")
    return ean_pairs


def delete_failed_eans(
    client: bigquery.Client,
    destination_table: str,
    eans: list[str],
) -> None:
    """
    Delete specific EANs with status='failed' from destination table.

    Used before reprocessing to remove old failed records.

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)
        eans: List of EANs to delete

    Raises:
        google.cloud.exceptions.GoogleCloudError: If delete fails
    """
    if not eans:
        logger.warning("No EANs provided for deletion")
        return

    # Build IN clause
    eans_list = "', '".join(eans)
    query = f"""
        DELETE FROM `{destination_table}`
        WHERE ean IN ('{eans_list}')
        AND status = 'failed'
    """

    logger.info(f"Deleting {len(eans)} failed EANs from {destination_table}")
    query_job = client.query(query)
    query_job.result()  # Wait for completion

    logger.info(f"Successfully deleted {len(eans)} failed EANs")


def fetch_batch_for_image_download(
    client: bigquery.Client,
    destination_table: str,
    batch_number: int,
    reprocess_failed: bool = False,
) -> list[dict]:
    """
    Fetch ALL EANs from a specific batch that need image download.

    Fetches rows where:
    - batch_number = X
    - status = 'processed'
    - images_download_status filter based on mode:
      - Normal mode: IS NULL (pending)
      - Reprocess mode: = 'failed' (retry failed)

    The JOIN with product_table/product_mediation is optional (LEFT JOIN) to get
    old UUIDs for change detection. EANs that don't exist in product_table yet
    (due to sync lag) will still be returned with NULL old_uuids, causing their
    images to be downloaded.

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)
        batch_number: Batch number to fetch
        reprocess_failed: If True, fetch failed downloads; if False, fetch pending

    Returns:
        List of dicts with keys: ean, json_raw, old_recto_image_uuid,
        old_verso_image_uuid

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    status_filter = (
        "images_download_status = 'failed'"
        if reprocess_failed
        else "images_download_status IS NULL"
    )

    # Convert provider IDs list to SQL IN clause
    provider_ids_str = ",".join(map(str, TITELIVE_PROVIDER_IDS))

    # Note: Provider filter is in JOIN condition (not WHERE) so that EANs
    # not yet synced to product_table are still returned with NULL old_uuids
    query = f"""
        SELECT
            dest.ean,
            dest.json_raw,
            MAX(IF(
                pm.imagetype = 'RECTO',
                pm.uuid,
                NULL
            )) as old_recto_image_uuid,
            MAX(IF(
                pm.imagetype = 'VERSO',
                pm.uuid,
                NULL
            )) as old_verso_image_uuid
        FROM `{destination_table}` dest
        LEFT JOIN `{PRODUCT_TABLE}` p
            ON dest.ean = p.ean
            AND p.lastproviderid IN ({provider_ids_str})
        LEFT JOIN `{PRODUCT_MEDIATION_TABLE}` pm
            ON p.id = pm.productid
            AND p.lastproviderid = pm.lastproviderid
        WHERE TRUE
            AND dest.batch_number = {batch_number}
            AND dest.status = 'processed'
            AND {status_filter}
        GROUP BY dest.ean, dest.json_raw
        ORDER BY dest.ean
    """

    mode_label = "failed" if reprocess_failed else "pending"
    logger.info(
        f"Fetching batch {batch_number} ({mode_label}) "
        f"for image download from {destination_table}"
    )
    query_job = client.query(query)
    results = query_job.result()

    rows = [
        {
            "ean": row.ean,
            "json_raw": row.json_raw,
            "old_recto_image_uuid": row.old_recto_image_uuid,
            "old_verso_image_uuid": row.old_verso_image_uuid,
        }
        for row in results
    ]
    logger.info(
        f"Fetched {len(rows)} {mode_label} EANs "
        f"from batch {batch_number} for image download"
    )
    return rows


def count_pending_image_downloads(
    client: bigquery.Client,
    destination_table: str,
) -> int:
    """
    Count EANs that need image download.

    Counts rows where:
    - status = 'processed'
    - images_download_status IS NULL

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)

    Returns:
        Number of pending image downloads

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT COUNT(*) as total
        FROM `{destination_table}`
        WHERE status = 'processed'
        AND images_download_status IS NULL
    """

    logger.info(f"Counting pending image downloads in {destination_table}")
    query_job = client.query(query)
    result = query_job.result()
    total = next(iter(result)).total

    logger.info(f"Found {total} pending image downloads")
    return total


def count_failed_image_downloads(
    client: bigquery.Client,
    destination_table: str,
) -> int:
    """
    Count EANs with failed image downloads.

    Counts rows where:
    - status = 'processed'
    - images_download_status = 'failed'

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)

    Returns:
        Number of failed image downloads

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT COUNT(*) as total
        FROM `{destination_table}`
        WHERE status = 'processed'
        AND images_download_status = 'failed'
    """

    logger.info(f"Counting failed image downloads in {destination_table}")
    query_job = client.query(query)
    result = query_job.result()
    total = next(iter(result)).total

    logger.info(f"Found {total} failed image downloads")
    return total


def update_image_download_results(
    client: bigquery.Client,
    destination_table: str,
    results: list[dict],
) -> None:
    """
    Update image download status for multiple EANs using temp table + MERGE.

    Uses temporary table strategy to avoid building large CASE statements.
    Scalable to any batch size.

    Args:
        client: BigQuery client
        destination_table: Full destination table ID (project.dataset.table)
        results: List of dicts with keys: ean, images_download_status,
        images_download_processed_at, recto_image_uuid (optional),
        verso_image_uuid (optional)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If update fails
    """
    if not results:
        logger.warning("No results to update")
        return

    logger.info(f"Updating image download status for {len(results)} EANs using MERGE")

    # Generate unique temp table name
    import uuid

    temp_table_suffix = str(uuid.uuid4()).replace("-", "_")
    project, dataset, _ = destination_table.split(".")
    temp_table_id = f"{project}.{dataset}.temp_image_status_{temp_table_suffix}"

    try:
        # Convert results to DataFrame
        df = pd.DataFrame(results)

        # Define temp table schema
        temp_schema = [
            bigquery.SchemaField("ean", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("images_download_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField(
                "images_download_processed_at", "TIMESTAMP", mode="REQUIRED"
            ),
            bigquery.SchemaField("recto_image_uuid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("verso_image_uuid", "STRING", mode="NULLABLE"),
        ]

        # Create temp table
        temp_table = bigquery.Table(temp_table_id, schema=temp_schema)
        temp_table.expires = None  # Manual cleanup
        logger.debug(f"Creating temp table: {temp_table_id}")
        client.create_table(temp_table)

        # Load data to temp table
        job_config = bigquery.LoadJobConfig(schema=temp_schema)
        load_job = client.load_table_from_dataframe(
            df, temp_table_id, job_config=job_config
        )
        load_job.result()  # Wait for load
        logger.debug(f"Loaded {len(results)} rows to temp table")

        # Execute MERGE statement
        merge_query = f"""
            MERGE `{destination_table}` AS target
            USING `{temp_table_id}` AS source
            ON target.ean = source.ean
            WHEN MATCHED THEN UPDATE SET
                images_download_status = source.images_download_status,
                images_download_processed_at = source.images_download_processed_at,
                recto_image_uuid = source.recto_image_uuid,
                verso_image_uuid = source.verso_image_uuid
        """

        logger.debug("Executing MERGE statement")
        merge_job = client.query(merge_query)
        merge_job.result()  # Wait for completion

        logger.info(f"Successfully updated {len(results)} EANs via MERGE")

    finally:
        # Clean up temp table
        try:
            logger.debug(f"Deleting temp table: {temp_table_id}")
            client.delete_table(temp_table_id, not_found_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete temp table {temp_table_id}: {e}")


def deduplicate_table_by_ean(
    client: bigquery.Client,
    table_id: str,
) -> None:
    """
    Deduplicate table by EAN, keeping most recent processed_at record.

    Uses CREATE OR REPLACE if table is already clustered by 'ean'.
    Falls back to temp table strategy if table has different clustering/partitioning.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If deduplication fails
    """
    logger.info(f"Deduplicating table {table_id} by EAN")

    # Try CREATE OR REPLACE first (works if table already has EAN clustering)
    deduplicate_query = f"""
        CREATE OR REPLACE TABLE `{table_id}`
        CLUSTER BY ean
        AS
        SELECT * FROM `{table_id}`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ean ORDER BY processed_at DESC) = 1
    """

    deduplicate_job = client.query(deduplicate_query)
    deduplicate_job.result()
    logger.info(f"Successfully deduplicated table {table_id}")


def count_table_rows(client: bigquery.Client, table_id: str) -> int:
    """
    Count the number of rows in a BigQuery table.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)

    Returns:
        Number of rows in the table

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
    query_job = client.query(query)
    result = query_job.result()
    row_count = next(iter(result)).row_count
    logger.info(f"Table {table_id} has {row_count:,} rows")
    return row_count


def get_status_breakdown(
    client: bigquery.Client,
    table_id: str,
) -> dict[str, int]:
    """
    Get row counts grouped by status field.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)

    Returns:
        Dict mapping status to count, e.g. {"processed": 1000, "failed": 50}

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT
            status,
            COUNT(*) as count
        FROM `{table_id}`
        GROUP BY status
        ORDER BY count DESC
    """
    query_job = client.query(query)
    results = query_job.result()
    return {row.status: row.count for row in results}


def get_images_status_breakdown(
    client: bigquery.Client,
    table_id: str,
) -> dict[str, int]:
    """
    Get row counts grouped by images_download_status field.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)

    Returns:
        Dict mapping status to count, e.g. {"processed": 1000, "NULL": 500}
        NULL status is represented as string "NULL"

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT
            IFNULL(images_download_status, 'NULL') as status,
            COUNT(*) as count
        FROM `{table_id}`
        GROUP BY images_download_status
        ORDER BY count DESC
    """
    query_job = client.query(query)
    results = query_job.result()
    return {row.status: row.count for row in results}


def get_sample_eans_by_status(
    client: bigquery.Client,
    table_id: str,
    status: str,
    limit: int = 5,
) -> list[str]:
    """
    Get sample EANs for a given status.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        status: Status value to filter by
        limit: Maximum number of EANs to return (default 5)

    Returns:
        List of EAN strings

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    query = f"""
        SELECT ean
        FROM `{table_id}`
        WHERE status = '{status}'
        LIMIT {limit}
    """
    query_job = client.query(query)
    results = query_job.result()
    return [row.ean for row in results]


def get_sample_eans_by_images_status(
    client: bigquery.Client,
    table_id: str,
    status: str | None,
    limit: int = 5,
) -> list[str]:
    """
    Get sample EANs for a given images_download_status.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        status: Status value to filter by (None for NULL status)
        limit: Maximum number of EANs to return (default 5)

    Returns:
        List of EAN strings

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    if status is None or status == "NULL":
        status_filter = "images_download_status IS NULL"
    else:
        status_filter = f"images_download_status = '{status}'"

    query = f"""
        SELECT ean
        FROM `{table_id}`
        WHERE {status_filter}
        LIMIT {limit}
    """
    query_job = client.query(query)
    results = query_job.result()
    return [row.ean for row in results]


def get_eans_not_in_product_table(
    client: bigquery.Client,
    destination_table: str,
    batch_number: int,
    provider_ids: list[int] = TITELIVE_PROVIDER_IDS,
    limit: int = 5,
) -> tuple[int, list[str]]:
    """
    Count and sample EANs in destination table that don't exist in product_table.

    Used for logging to understand how many EANs haven't synced to backend yet.

    Args:
        client: BigQuery client
        destination_table: Full destination table ID
        batch_number: Batch number to check
        provider_ids: Provider IDs to filter by
        limit: Maximum number of sample EANs to return

    Returns:
        Tuple of (count, sample_eans)

    Raises:
        google.cloud.exceptions.GoogleCloudError: If query fails
    """
    provider_ids_str = ",".join(map(str, provider_ids))

    # Count query
    count_query = f"""
        SELECT COUNT(*) as count
        FROM `{destination_table}` dest
        LEFT JOIN `{PRODUCT_TABLE}` p
            ON dest.ean = p.ean
            AND p.lastproviderid IN ({provider_ids_str})
        WHERE dest.batch_number = {batch_number}
            AND dest.status = 'processed'
            AND p.ean IS NULL
    """
    count_job = client.query(count_query)
    count_result = count_job.result()
    count = next(iter(count_result)).count

    # Sample query
    sample_query = f"""
        SELECT dest.ean
        FROM `{destination_table}` dest
        LEFT JOIN `{PRODUCT_TABLE}` p
            ON dest.ean = p.ean
            AND p.lastproviderid IN ({provider_ids_str})
        WHERE dest.batch_number = {batch_number}
            AND dest.status = 'processed'
            AND p.ean IS NULL
        LIMIT {limit}
    """
    sample_job = client.query(sample_query)
    sample_result = sample_job.result()
    sample_eans = [row.ean for row in sample_result]

    return count, sample_eans
