import logging
from datetime import datetime

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def save_to_historical(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table_name: str,
    end_date: datetime,
    schema: list[bigquery.SchemaField],
):
    """Uploads partitioned DataFrame to BigQuery."""
    client = bigquery.Client()
    date_str = end_date.strftime("%Y%m%d")
    base_table_id = f"{project}.{dataset}.{table_name}_histo"
    table_id = f"{base_table_id}${date_str}"

    # Ensure table exists with partitioning
    try:
        client.get_table(base_table_id)
    except NotFound:
        tbl = bigquery.Table(
            base_table_id,
            schema=schema,
        )
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="update_date"
        )
        client.create_table(tbl)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="update_date"
        ),
    )

    logger.info(f"Loading {len(df)} rows into {table_id}")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


def save_transactional_to_historical(
    df: pd.DataFrame,
    schema: list[bigquery.SchemaField],
    project: str,
    dataset: str,
    table_name: str,
    date_suffix: datetime,
):
    """Saves transactional data with a date-prefixed table name."""
    client = bigquery.Client()
    yyyymmdd = date_suffix.strftime("%Y%m%d")
    table_id = f"{project}.{dataset}.{yyyymmdd}_{table_name}_histo"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=schema,
    )

    logger.info(f"Appending {len(df)} rows to {table_id}")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
