import asyncio
import logging
from datetime import datetime

from workflows.brevo import extract, load, transform
from workflows.brevo.config import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    TRANSACTIONAL_TABLE_NAME,
)

logger = logging.getLogger(__name__)


def run_newsletter_pipeline(
    connector, audience: str, table_name: str, end_date: datetime
):
    """
    Orchestrates the Newsletter ETL pipeline.
    """
    logger.info(f"🎬 Starting Newsletter Pipeline for {audience}...")

    # 1. Extract
    api_campaigns = extract.fetch_campaigns(connector)
    if not api_campaigns:
        logger.info("No campaigns found. Pipeline finished.")
        return

    # 2. Transform
    df_clean = transform.transform_campaigns_to_dataframe(
        api_campaigns, audience, update_date=end_date
    )

    # 3. Load
    load.load_campaigns_to_bq(
        df=df_clean,
        project=GCP_PROJECT,
        dataset=BIGQUERY_RAW_DATASET,
        table_name=table_name,
        end_date=end_date,
    )
    logger.info("🎬 Newsletter Pipeline finished successfully.")


def run_transactional_pipeline(
    connector,
    audience: str,
    start_date: datetime,
    end_date: datetime,
    async_mode: bool = False,
):
    """
    Orchestrates the Transactional Events ETL pipeline.
    """
    logger.info(
        f"🎬 Starting Transactional Pipeline (Async={async_mode}) for {audience}..."
    )

    # 1. Extract
    if async_mode:
        raw_events_dicts = asyncio.run(
            extract.fetch_transactional_events_async(
                connector, start_date, end_date, env=ENV_SHORT_NAME
            )
        )
    else:
        raw_events_dicts = extract.fetch_transactional_events(
            connector, start_date, end_date, env=ENV_SHORT_NAME
        )

    if not raw_events_dicts:
        logger.info("No transactional events found. Pipeline finished.")
        return

    # 2. Transform
    df_clean = transform.transform_events_to_dataframe(raw_events_dicts, audience)

    # 3. Load
    load.load_transactional_events_to_bq(
        df=df_clean,
        project=GCP_PROJECT,
        dataset=BIGQUERY_TMP_DATASET,
        table_name=TRANSACTIONAL_TABLE_NAME,
        end_date=end_date,
    )
    logger.info("🎬 Transactional Pipeline finished successfully.")
