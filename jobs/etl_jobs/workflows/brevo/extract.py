import asyncio
import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd
from connectors.brevo.schemas import ApiCampaign, ApiEvent, ApiTemplatesResponse
from http_tools.circuit_breakers import CircuitBreakerOpenError
from http_tools.exceptions import HttpClientError, RateLimitError, ServerError
from pydantic import ValidationError

logger = logging.getLogger(__name__)


def fetch_campaigns(connector) -> List[ApiCampaign]:
    """
    Fetches email campaigns from Brevo API and validates them against the Read Schema.
    """
    logger.info("🚀 [Sync] Fetching email campaigns...")

    try:
        resp = connector.get_email_campaigns()
    except CircuitBreakerOpenError:
        logger.error(
            "🛑 Circuit breaker is open for Brevo API. "
            "Too many recent failures. Job will retry later."
        )
        raise
    except RateLimitError as e:
        logger.error(
            f"🛑 Rate limit exceeded after retries. " f"Retry-After: {e.retry_after}s. "
        )
        raise
    except ServerError as e:
        logger.error(
            f"🛑 Brevo server error (5xx) after all retries. "
            f"Status: {e.status_code}, URL: {e.url}"
        )
        raise
    except HttpClientError as e:
        logger.error(f"🛑 HTTP error after retries: {e}")
        raise

    raw_campaigns = resp.json().get("campaigns", [])
    logger.info(f"Fetched {len(raw_campaigns)} campaigns.")

    if not raw_campaigns:
        return []

    validated_campaigns = []
    for camp in raw_campaigns:
        try:
            validated_campaigns.append(ApiCampaign(**camp))
        except ValidationError as e:
            logger.error(
                f"🛑 Schema validation failed for campaign {camp.get('id')}: {e}"
            )
            raise e

    return validated_campaigns


def fetch_transactional_events(
    connector, start_date: datetime, end_date: datetime, env: str = "prod"
) -> List[Dict]:
    """
    Fetches transactional events (sync) and validates them against the Read Schema.
    Returns a list of dictionaries with enriched metadata (template_id, tag).
    """
    logger.info("🚀 [Sync] Fetching transactional events...")

    # 1. Fetch Templates
    try:
        templates_resp = connector.get_smtp_templates(active_only=True)
    except Exception as e:
        logger.error(f"🛑 Failed to fetch SMTP templates: {e}")
        raise

    try:
        validated_templates_resp = ApiTemplatesResponse(**templates_resp.json())
        templates = validated_templates_resp.templates
    except ValidationError as e:
        logger.error(f"🛑 Schema validation failed for SMTP templates: {e}")
        raise

    if not templates:
        logger.warning("No active SMTP templates found.")
        return []

    if env != "prod":
        templates = templates[:1]

    all_events = []
    failed_templates = []
    logger.info(f"Processing {len(templates)} templates sequentially...")

    for template in templates:
        t_id, tag = template.id, template.tag

        for event_type in ["delivered", "opened", "unsubscribed"]:
            offset = 0
            while True:
                logger.info(
                    f"  [Sync] Fetching {event_type} for template {t_id} (offset {offset})"
                )

                try:
                    resp = connector.get_email_event_report(
                        template_id=t_id,
                        event=event_type,
                        start_date=start_date.strftime("%Y-%m-%d"),
                        end_date=end_date.strftime("%Y-%m-%d"),
                        offset=offset,
                    )
                except Exception as e:
                    logger.error(
                        f"⚠️ Failed to fetch {event_type} for template {t_id}: {e}"
                    )
                    failed_templates.append((t_id, event_type))
                    break

                events = resp.json().get("events", [])
                if not events:
                    break

                for e in events:
                    try:
                        validated_event = ApiEvent(**e)
                        # We return a dict here because we are enriching with template info
                        # Ideally, we should have a DTO or pass the tuple, but keeping close to original logic for now
                        all_events.append(
                            {
                                "template": t_id,
                                "tag": tag,
                                "email": validated_event.email,
                                "event": validated_event.event,
                                "event_date": pd.to_datetime(
                                    validated_event.date
                                ).date(),
                            }
                        )
                    except ValidationError as err:
                        logger.error(
                            f"🛑 Schema validation failed for event {e}: {err}"
                        )
                        raise err

                if len(events) < 2500:
                    break
                offset += 2500

    if failed_templates:
        logger.warning(
            f"⚠️ Failed to fetch data for {len(failed_templates)} template/event combinations."
        )

    return all_events


async def fetch_transactional_events_async(
    connector, start_date: datetime, end_date: datetime, env: str = "prod"
) -> List[Dict]:
    """
    Fetches transactional events (async) and validates them against the Read Schema.
    """
    logger.info("🚀 [Async] Fetching transactional events...")

    try:
        templates_resp = await connector.get_smtp_templates(active_only=True)
    except Exception as e:
        logger.error(f"🛑 Failed to fetch SMTP templates: {e}")
        raise

    try:
        validated_templates_resp = ApiTemplatesResponse(**templates_resp.json())
        templates = validated_templates_resp.templates
    except ValidationError as e:
        logger.error(f"🛑 Schema validation failed for SMTP templates: {e}")
        raise

    if not templates:
        logger.warning("No templates to process.")
        return []

    if env != "prod":
        templates = templates[:2]

    logger.info(f"Scheduling parallel fetch for {len(templates)} templates...")

    tasks = [
        _fetch_template_events_async(connector, t, start_date, end_date)
        for t in templates
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_events = []
    failed_templates = []

    for i, res in enumerate(results):
        template_id = templates[i].id
        if isinstance(res, Exception):
            logger.error(f"❌ Template {template_id}: Error - {res}")
            failed_templates.append(template_id)
        elif isinstance(res, list):
            all_events.extend(res)

    logger.info(
        f"📊 Processed {len(templates) - len(failed_templates)}/{len(templates)} templates successfully."
    )

    return all_events


async def _fetch_template_events_async(
    connector, template, start_dt: datetime, end_dt: datetime
) -> List[Dict]:
    """
    Helper for async fetch.
    """
    t_id, tag = template.id, template.tag
    event_types = ["delivered", "opened", "unsubscribed"]

    async def fetch_type_pages(etype: str) -> List[Dict]:
        results = []
        offset = 0
        while True:
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
                try:
                    validated_event = ApiEvent(**e)
                    results.append(
                        {
                            "template": t_id,
                            "tag": tag,
                            "email": validated_event.email,
                            "event": validated_event.event,
                            "event_date": pd.to_datetime(validated_event.date).date(),
                        }
                    )
                except ValidationError as err:
                    logger.error(f"🛑 Schema validation failed for event {e}: {err}")
                    raise err

            if len(data) < 2500:
                break
            offset += 2500
        return results

    type_results = await asyncio.gather(*[fetch_type_pages(et) for et in event_types])
    return [item for sublist in type_results for item in sublist]
