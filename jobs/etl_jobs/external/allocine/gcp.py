import logging

from google.cloud import bigquery, secretmanager, storage
from google.cloud.bigquery import LoadJobConfig, SchemaField, WriteDisposition

from constants import BQ_LOCATION, SECRET_VERSION
from data import RAW_SCHEMA, STAGING_SCHEMA

logger = logging.getLogger(__name__)


def get_secret(project_id: str, secret_id: str, version: str = SECRET_VERSION) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8").strip()


def get_bq_client(project_id: str) -> bigquery.Client:
    return bigquery.Client(project=project_id, location=BQ_LOCATION)


def truncate_and_load_staging(
    rows: list[dict],
    project_id: str,
    dataset: str,
    staging_table: str,
    bq_client: bigquery.Client | None = None,
) -> None:
    client = bq_client or get_bq_client(project_id)
    table_id = f"{project_id}.{dataset}.{staging_table}"
    job_config = LoadJobConfig(
        schema=STAGING_SCHEMA,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows into %s (truncated).", len(rows), table_id)


def _ensure_raw_table(
    project_id: str, dataset: str, raw_table: str, client: bigquery.Client, raw_schema: list[SchemaField] = RAW_SCHEMA
) -> None:
    table_id = f"{project_id}.{dataset}.{raw_table}"
    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=raw_schema)
        client.create_table(table)
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

    # All staging fields except movie_id are updatable
    update_fields = [f.name for f in STAGING_SCHEMA if f.name != "movie_id"]
    update_set = ",\n      ".join(f"T.{f} = S.{f}" for f in update_fields)
    # Poster trigger: if poster_url changed, re-arm download and reset retries
    update_set += """,
      T.poster_to_download = CASE
        WHEN IFNULL(T.poster_url, '') != IFNULL(S.poster_url, '') THEN TRUE
        ELSE T.poster_to_download
      END,
      T.retry_count = CASE
        WHEN IFNULL(T.poster_url, '') != IFNULL(S.poster_url, '') THEN 0
        ELSE T.retry_count
      END"""

    insert_cols = ", ".join(f.name for f in STAGING_SCHEMA) + ", poster_to_download, poster_gcs_path, retry_count"
    insert_vals = ", ".join(f"S.{f.name}" for f in STAGING_SCHEMA) + ", TRUE, NULL, 0"

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
    bq_client: bigquery.Client | None = None,
) -> list[dict]:
    client = bq_client or get_bq_client(project_id)
    table_ref = f"`{project_id}.{dataset}.{raw_table}`"
    sql = f"""
    SELECT movie_id, poster_url
    FROM {table_ref}
    WHERE poster_to_download = TRUE
      AND retry_count < {max_retries}
      AND poster_url IS NOT NULL
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
        retry_count = 0
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


def upload_to_gcs(
    bucket_name: str,
    blob_name: str,
    content: bytes,
    content_type: str,
    gcs_client: storage.Client | None = None,
) -> str:
    client = gcs_client or storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(content, content_type=content_type)
    return f"gs://{bucket_name}/{blob_name}"
