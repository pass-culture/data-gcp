import logging
import mimetypes
import uuid
from pathlib import PurePosixPath
from urllib.parse import urlparse

from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SchemaField, WriteDisposition
from google.cloud.exceptions import NotFound

from gcp import get_bq_client
from schema import RAW_EXTRA_INIT_VALUES, RAW_SCHEMA, STAGING_SCHEMA

logger = logging.getLogger(__name__)


def _poster_uuid(poster_url: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, poster_url))


def _detect_extension(url: str, content_type: str | None) -> str:
    suffix = PurePosixPath(urlparse(url).path).suffix
    if suffix:
        return suffix.lstrip(".")
    if content_type:
        ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext:
            return ext.lstrip(".")
    return "jpg"


def poster_blob_name(prefix: str, poster_url: str, content_type: str | None) -> str:
    return f"{prefix}/{_poster_uuid(poster_url)}.{_detect_extension(poster_url, content_type)}"


def load_staging_table(
    rows: list[dict],
    project_id: str,
    dataset: str,
    staging_table: str,
    bq_client: bigquery.Client | None = None,
) -> None:
    client = bq_client or get_bq_client(project_id)
    table_id = f"{project_id}.{dataset}.{staging_table}"

    # 1. Ensure the table exists
    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=STAGING_SCHEMA))
        logger.info("Created staging table %s.", table_id)

    # 2. Load extracted data into staging
    job_config = LoadJobConfig(
        schema=STAGING_SCHEMA,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows into %s (truncated).", len(rows), table_id)


def clear_staging_table(
    project_id: str, dataset: str, staging_table: str, bq_client: bigquery.Client | None = None
) -> None:
    client = bq_client or get_bq_client(project_id)
    table_id = f"{project_id}.{dataset}.{staging_table}"
    try:
        client.delete_table(table_id)
        logger.info("Deleted staging table %s.", table_id)
    except Exception:
        logger.warning("Staging table %s did not exist, nothing to delete.", table_id)


def _ensure_raw_table(
    project_id: str,
    dataset: str,
    raw_table: str,
    client: bigquery.Client,
    raw_schema: list[SchemaField] = RAW_SCHEMA,
) -> None:
    table_id = f"{project_id}.{dataset}.{raw_table}"
    try:
        table = client.get_table(table_id)
        # Validate schema
        existing_cols = {f.name for f in table.schema}
        missing = {f.name for f in raw_schema} - existing_cols
        if missing:
            raise RuntimeError(f"Raw table exists but is missing columns: {missing}")
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=raw_schema))
        logger.info("Created table %s.", table_id)


def merge_staging_to_raw(
    project_id: str,
    dataset: str,
    staging_table: str,
    raw_table: str,
    bq_client: bigquery.Client | None = None,
) -> dict[str, int]:
    client = bq_client or get_bq_client(project_id)
    _ensure_raw_table(project_id, dataset, raw_table, client)

    staging_ref = f"`{project_id}.{dataset}.{staging_table}`"
    raw_ref = f"`{project_id}.{dataset}.{raw_table}`"

    update_fields = [f.name for f in STAGING_SCHEMA if f.name != "movie_id"]
    update_set = ",\n      ".join(f"T.{f} = S.{f}" for f in update_fields)
    update_set += """,
      T.poster_to_download = CASE
        WHEN IFNULL(T.poster_url, '') != IFNULL(S.poster_url, '') THEN TRUE
        ELSE T.poster_to_download
      END,
      T.retry_count = CASE
        WHEN IFNULL(T.poster_url, '') != IFNULL(S.poster_url, '') THEN 0
        ELSE T.retry_count
      END,
      T.updated_at = CURRENT_TIMESTAMP()
      """

    staging_fields = {f.name for f in STAGING_SCHEMA}
    insert_cols = ", ".join(f.name for f in RAW_SCHEMA)
    insert_vals = ", ".join(
        f"S.{f.name}" if f.name in staging_fields else RAW_EXTRA_INIT_VALUES[f.name] for f in RAW_SCHEMA
    )

    sql = f"""
    MERGE {raw_ref} T
    USING {staging_ref} S
    ON T.movie_id = S.movie_id
    WHEN MATCHED AND T.content_hash != S.content_hash THEN
      UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """

    logger.info("Running MERGE staging → raw...")
    job = client.query(sql)
    job.result()
    stats = job.dml_stats
    inserted = stats.inserted_row_count if stats else 0
    updated = stats.updated_row_count if stats else 0
    logger.info("MERGE complete — %d new, %d updated.", inserted, updated)
    return {"inserted": inserted, "updated": updated}


def fetch_pending_posters(
    project_id: str,
    dataset: str,
    raw_table: str,
    max_retries: int,
    poster_download_backoff: int,
    poster_download_backoff_unit: str,
    bq_client: bigquery.Client | None = None,
) -> list[dict]:
    client = bq_client or get_bq_client(project_id)
    table_ref = f"`{project_id}.{dataset}.{raw_table}`"
    sql = f"""
    SELECT movie_id, poster_url
    FROM {table_ref}
    WHERE poster_to_download = TRUE
      AND poster_url IS NOT NULL
      AND (
          retry_count = 0
          OR (
              retry_count < {max_retries}
              AND TIMESTAMP_TRUNC(updated_at, {poster_download_backoff_unit})
                  <= TIMESTAMP_SUB(
                      TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), {poster_download_backoff_unit}),
                      INTERVAL CAST(retry_count * {poster_download_backoff} AS INT64) {poster_download_backoff_unit}
                  )
          )
      )
    """
    return [dict(row) for row in client.query(sql).result()]


def update_poster_success(
    movie_id: str,
    gcs_path: str,
    project_id: str,
    dataset: str,
    raw_table: str,
    bq_client: bigquery.Client | None = None,
) -> None:
    client = bq_client or get_bq_client(project_id)
    table_ref = f"`{project_id}.{dataset}.{raw_table}`"
    sql = f"""
    UPDATE {table_ref}
    SET poster_to_download = FALSE,
        poster_gcs_path = @gcs_path,
        retry_count = 0,
        updated_at = CURRENT_TIMESTAMP()
    WHERE movie_id = @movie_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gcs_path", "STRING", gcs_path),
            bigquery.ScalarQueryParameter("movie_id", "STRING", movie_id),
        ]
    )
    client.query(sql, job_config=job_config).result()


def update_poster_failure(
    movie_id: str,
    project_id: str,
    dataset: str,
    raw_table: str,
    bq_client: bigquery.Client | None = None,
) -> None:
    client = bq_client or get_bq_client(project_id)
    table_ref = f"`{project_id}.{dataset}.{raw_table}`"
    sql = f"""
    UPDATE {table_ref}
    SET retry_count = retry_count + 1
    WHERE movie_id = @movie_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("movie_id", "STRING", movie_id),
        ]
    )
    client.query(sql, job_config=job_config).result()
