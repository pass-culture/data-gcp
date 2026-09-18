import logging
import os
from datetime import UTC, datetime

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

GCP_PROJECT = os.environ["GCP_PROJECT_ID"]


def save(
    df: pd.DataFrame,
    dataset_id: str,
    table_name: str,
    vintage_year: int,
    description: str,
) -> None:
    """Replace `dataset.table` with `df` (a referential is a full snapshot, not a log)."""
    df = df.copy()
    df["vintage_year"] = vintage_year
    df["imported_at"] = datetime.now(UTC)

    table_id = f"{GCP_PROJECT}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[bigquery.SchemaField(name, _bq_type(df[name])) for name in df.columns],
    )
    logger.info("Loading %s rows into %s", len(df), table_id)
    client = bigquery.Client()
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

    table = client.get_table(table_id)
    table.description = description
    client.update_table(table, ["description"])


def _bq_type(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT"
    return "STRING"
