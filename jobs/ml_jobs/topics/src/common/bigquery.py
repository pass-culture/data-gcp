"""BigQuery read/write helpers shared by every domain.

``pandas.read_gbq``/``to_gbq`` can't bind query parameters (needed for
section 5's ``@dim``/``@gtl_ids``-style queries) and don't speak polars, so
these go through the ``google.cloud.bigquery`` client directly.
"""

import io

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
