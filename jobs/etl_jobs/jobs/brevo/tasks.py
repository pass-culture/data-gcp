import asyncio
import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd

# Internal imports
from jobs.brevo.config import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    TRANSACTIONAL_TABLE_NAME,
    campaigns_histo_schema,
    transactional_histo_schema,
)
from jobs.brevo.load import save_to_historical, save_transactional_to_historical
from jobs.brevo.transform import (
    transform_campaigns_to_dataframe,
    transform_events_to_dataframe,
)

logger = logging.getLogger(__name__)

# ==========================================
# 1. NEWSLETTER TASK (Sync)
# ==========================================


def run_newsletter_etl(connector, audience: str, table_name: str, end_date: datetime):
    """ETL pipeline for newsletter campaigns."""
    logger.info(f"🚀 [Sync] Starting Newsletter ETL for audience: {audience}")

    resp = connector.get_email_campaigns()
    if not resp:
        logger.warning("No response received from Brevo for campaigns.")
        return

    campaigns = resp.json().get("campaigns", [])
    logger.info(f"Fetched {len(campaigns)} campaigns.")

    if not campaigns:
        return

    # Transform
    df = transform_campaigns_to_dataframe(campaigns, audience, update_date=end_date)

    # Load
    save_to_historical(
        df=df,
        schema=campaigns_histo_schema,
        project=GCP_PROJECT,
        dataset=BIGQUERY_RAW_DATASET,
        table_name=table_name,
        end_date=end_date,
    )
    logger.info("✅ Newsletter ETL finished.")


# ==========================================
# 2. TRANSACTIONAL TASK (Sync)
# ==========================================


def run_transactional_etl(
    connector, audience: str, start_date: datetime, end_date: datetime
):
    """ETL pipeline for transactional emails (Sync version)."""
    logger.info(f"🚀 [Sync] Starting Transactional ETL for audience: {audience}")

    templates_resp = connector.get_smtp_templates(active_only=True)
    templates = templates_resp.json().get("templates", []) if templates_resp else []

    if not templates:
        logger.warning("No active SMTP templates found.")
        return

    if ENV_SHORT_NAME != "prod":
        templates = templates[:1]

    all_events = []
    logger.info(f"Processing {len(templates)} templates sequentially...")

    for template in templates:
        t_id, tag = template.get("id"), template.get("tag")

        for event_type in ["delivered", "opened", "unsubscribed"]:
            offset = 0
            while True:
                logger.info(
                    f"  [Sync] Fetching {event_type} for template {t_id} (offset {offset})"
                )
                resp = connector.get_email_event_report(
                    template_id=t_id,
                    event=event_type,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    offset=offset,
                )

                if not resp:
                    break
                events = resp.json().get("events", [])
                if not events:
                    break

                for e in events:
                    all_events.append(
                        {
                            "template": t_id,
                            "tag": tag,
                            "email": e.get("email"),
                            "event": e.get("event"),
                            "event_date": pd.to_datetime(e.get("date")).date(),
                        }
                    )

                if len(events) < 2500:
                    break
                offset += 2500

    # 3. Transform & Load
    if all_events:
        logger.info(f"Transforming and saving {len(all_events)} records...")
        df_final = transform_events_to_dataframe(all_events, audience)
        save_transactional_to_historical(
            df_final,
            transactional_histo_schema,
            GCP_PROJECT,
            BIGQUERY_TMP_DATASET,
            TRANSACTIONAL_TABLE_NAME,
            datetime.today(),
        )


# ==========================================
# 3. TRANSACTIONAL TASK (Async)
# ==========================================


async def run_async_transactional_etl(
    connector, audience: str, start_date: datetime, end_date: datetime
):
    """
    High-Performance Async ETL.
    Removes the manual 'batch_size' bottleneck and uses Client-side queueing.
    """
    logger.info(
        f"🚀 [Async] Starting High-Speed Transactional ETL for audience: {audience}"
    )

    # 1. Fetch Templates
    templates_resp = await connector.get_smtp_templates(active_only=True)
    if not templates_resp:
        return
    templates = templates_resp.json().get("templates", [])

    if not templates:
        logger.warning("No templates to process.")
        return

    if ENV_SHORT_NAME != "prod":
        templates = templates[:2]

    logger.info(f"Scheduling parallel fetch for {len(templates)} templates...")

    # --- PERFORMANCE FIX ---
    # We schedule ALL template tasks immediately.
    # Your 'AsyncHttpClient' already has a semaphore/limiter.
    # It will automatically queue them without us needing manual 'batch' loops.
    tasks = [
        _fetch_template_events_async(connector, t, start_date, end_date)
        for t in templates
    ]

    # gather() starts all tasks. Results return in a list.
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_events = []
    for res in results:
        if isinstance(res, list):
            all_events.extend(res)
        else:
            logger.error(f"❌ Template fetch failed with error: {res}")

    # 3. Transform & Load
    if all_events:
        logger.info(
            f"📊 Collected {len(all_events)} events. Transformation starting..."
        )
        df_final = transform_events_to_dataframe(all_events, audience)
        save_transactional_to_historical(
            df_final,
            transactional_histo_schema,
            GCP_PROJECT,
            BIGQUERY_TMP_DATASET,
            TRANSACTIONAL_TABLE_NAME,
            datetime.today(),
        )
    logger.info("✅ Async Transactional ETL complete.")


async def _fetch_template_events_async(
    connector, template: Dict, start_dt: datetime, end_dt: datetime
) -> List[Dict]:
    """
    Fetches events for a template.
    Parallelizes 'delivered', 'opened', and 'unsubscribed' for maximum speed.
    """
    t_id, tag = template.get("id"), template.get("tag")

    # --- PERFORMANCE FIX ---
    # We fetch all 3 event types for this template in parallel instead of sequentially.
    event_types = ["delivered", "opened", "unsubscribed"]

    async def fetch_type_pages(etype: str) -> List[Dict]:
        results = []
        offset = 0
        while True:
            # Heartbeat logging restored
            logger.info(
                f"    [Async] Template {t_id} | Fetching {etype} | Offset {offset}"
            )

            resp = await connector.get_email_event_report(
                template_id=t_id,
                event=etype,
                start_date=start_dt.strftime("%Y-%m-%d"),
                end_date=end_dt.strftime("%Y-%m-%d"),
                offset=offset,
            )

            if not resp:
                break
            data = resp.json().get("events", [])
            if not data:
                break

            for e in data:
                results.append(
                    {
                        "template": t_id,
                        "tag": tag,
                        "email": e.get("email"),
                        "event": e.get("event"),
                        "event_date": pd.to_datetime(e.get("date")).date(),
                    }
                )

            if len(data) < 2500:
                break
            offset += 2500
        return results

    # Schedule the 3 event types in parallel for THIS template
    type_results = await asyncio.gather(*[fetch_type_pages(et) for et in event_types])

    # Flatten results (list of lists to list)
    return [item for sublist in type_results for item in sublist]
