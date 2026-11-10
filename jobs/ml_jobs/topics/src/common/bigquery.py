"""BigQuery read/write helpers shared by every domain.

``pandas.read_gbq``/``to_gbq`` can't bind query parameters (needed for
section 5's ``@dim``/``@gtl_ids``-style queries) and don't speak polars, so
these go through the ``google.cloud.bigquery`` client directly.
"""

import io
import uuid

import polars as pl
from google.cloud import bigquery
from loguru import logger

from src.common.constants import GCP_PROJECT_ID

QueryParameter = bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter


def read_dataframe(
    sql: str, query_parameters: list[QueryParameter] | None = None
) -> pl.DataFrame:
    """Run a (optionally parameterized) query and return the result as a
    polars DataFrame.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters or [])
    logger.info(
        f"Running query ({len(sql)} chars, {len(query_parameters or [])} params)"
    )
    result = client.query(sql, job_config=job_config).result()
    return pl.from_arrow(result.to_arrow())


def write_dataframe(
    data: pl.DataFrame,
    dataset: str,
    table: str,
    *,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    schema: list[bigquery.SchemaField] | None = None,
) -> None:
    """Load a polars DataFrame into a BigQuery table via an in-memory parquet
    stream (no intermediate GCS file), mirroring
    ``jobs/ml_jobs/reduction/tools/utils.py::export_polars_to_bq``.

    An explicit ``schema`` keeps provenance columns (e.g. ``mlflow_run_id``)
    typed even when a chunk is empty or partial, where BigQuery's
    ``autodetect`` could otherwise guess wrong or fail.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    destination = f"{GCP_PROJECT_ID}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        schema=schema,
        autodetect=schema is None,
    )
    with io.BytesIO() as stream:
        data.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream, destination=destination, job_config=job_config
        )
    job.result()
    logger.info(f"Wrote {data.height} rows to {destination} ({write_disposition})")


def merge_upsert(
    data: pl.DataFrame,
    dataset: str,
    table: str,
    key_columns: list[str],
    schema: list[bigquery.SchemaField],
) -> None:
    """Upsert ``data`` into ``{dataset}.{table}``, matched on ``key_columns``:
    existing rows are replaced, new ones inserted.

    BigQuery has no "load with merge", so this loads ``data`` into a
    temporary staging table first, then runs one ``MERGE`` statement; the
    staging table is dropped afterward. Creates the destination table (from
    ``schema``) first if it doesn't exist yet, so a first incremental run
    ahead of any full refit still works.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    destination = f"{GCP_PROJECT_ID}.{dataset}.{table}"
    staging_table = f"{table}__staging_{uuid.uuid4().hex[:8]}"
    staging_destination = f"{GCP_PROJECT_ID}.{dataset}.{staging_table}"

    client.create_table(bigquery.Table(destination, schema=schema), exists_ok=True)
    write_dataframe(
        data,
        dataset,
        staging_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    columns = [field.name for field in schema]
    match_condition = " AND ".join(f"T.{col} = S.{col}" for col in key_columns)
    update_clause = ", ".join(
        f"{col} = S.{col}" for col in columns if col not in key_columns
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"S.{col}" for col in columns)
    merge_sql = f"""
        MERGE `{destination}` T
        USING `{staging_destination}` S
        ON {match_condition}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
    """

    try:
        client.query(merge_sql).result()
    finally:
        client.delete_table(staging_destination, not_found_ok=True)

    logger.info(f"Upserted {data.height} rows into {destination} (key: {key_columns})")
