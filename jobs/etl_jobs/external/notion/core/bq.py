"""BigQuery load for the dashboard-docs export → raw_<env>.notion_dashboard_docs."""

import logging
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from core.utils import BIGQUERY_RAW_DATASET, GCP_PROJECT, NOTION_DOCS_TABLE

logger = logging.getLogger(__name__)

BQ_SCHEMA = [
    bigquery.SchemaField("page_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("notion_url", "STRING"),
    bigquery.SchemaField("dashboard_id", "INTEGER"),
    bigquery.SchemaField("dashboard_url", "STRING"),
    bigquery.SchemaField("properties", "STRING"),
    bigquery.SchemaField("body_md", "STRING"),
    bigquery.SchemaField("last_edited", "TIMESTAMP"),
    bigquery.SchemaField("execution_date", "TIMESTAMP"),
]


def save_to_bq(rows: list, execution_date: datetime):
    if not rows:
        logger.warning("No rows to save — leaving %s untouched.", NOTION_DOCS_TABLE)
        return
    df = pd.DataFrame(rows)
    df["dashboard_id"] = df["dashboard_id"].astype("Int64")
    df["last_edited"] = pd.to_datetime(df["last_edited"], utc=True)
    df["execution_date"] = pd.to_datetime(df["execution_date"], utc=True)

    yyyymmdd = execution_date.strftime("%Y%m%d")
    table_id = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{NOTION_DOCS_TABLE}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
        ),
    )
    logger.info("Loading %s rows → %s", len(df), table_id)
    client = bigquery.Client(project=GCP_PROJECT)
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
