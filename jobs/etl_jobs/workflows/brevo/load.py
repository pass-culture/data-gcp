import logging
from datetime import datetime

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from utils.schemas import pydantic_to_bigquery_schema
from workflows.brevo.schemas import TableCampaign, TableTransactionalEvent

logger = logging.getLogger(__name__)


def load_campaigns_to_bq(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table_name: str,
    end_date: datetime,
):
    """
    Uploads partitioned DataFrame to BigQuery.
    Generates schema dynamically from TableCampaign (Write Contract).
    """
    if df.empty:
        logger.info("No campaign data to load.")
        return

    # Generate schema from Workflow Contract (Write Schema)
    bq_schema = pydantic_to_bigquery_schema(TableCampaign)

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
            schema=bq_schema,
        )
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="update_date"
        )
        client.create_table(tbl)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=bq_schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="update_date"
        ),
    )

    logger.info(f"Loading {len(df)} rows into {table_id}")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    logger.info("✅ Campaigns loaded successfully.")


def load_transactional_events_to_bq(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table_name: str,
    end_date: datetime,
):
    """
    Saves transactional data with a date-prefixed table name.
    Generates schema dynamically from TableTransactionalEvent (Write Contract).
    """
    if df.empty:
        logger.info("No transactional event data to load.")
        return

    # Generate schema from Workflow Contract (Write Schema)
    bq_schema = pydantic_to_bigquery_schema(TableTransactionalEvent)

    client = bigquery.Client()
    yyyymmdd = end_date.strftime("%Y%m%d")
    table_id = f"{project}.{dataset}.{yyyymmdd}_{table_name}_histo"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=bq_schema,
    )

    logger.info(f"Appending {len(df)} rows to {table_id}")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    logger.info("✅ Transactional events loaded successfully.")


# Aliases for backward compatibility during transition
save_to_historical = load_campaigns_to_bq
save_transactional_to_historical = load_transactional_events_to_bq
