import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# Rate limiters now imported from connector module
from connectors.brevo import (
    AsyncBrevoConnector,
    # Rate limiters available but not needed here anymore
    # SyncBrevoHeaderRateLimiter,
    # AsyncBrevoHeaderRateLimiter,
    BrevoConnector,
)
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager
from jobs.brevo.config import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    TRANSACTIONAL_TABLE_NAME,
    campaigns_histo_schema,
    transactional_histo_schema,
)

logger = logging.getLogger(__name__)


# ===== CUSTOM EXCEPTIONS =====


class BrevoETLError(Exception):
    """Base exception for Brevo ETL operations."""

    pass


class BrevoAPIError(BrevoETLError):
    """Exception raised when Brevo API calls fail."""

    pass


class BrevoDataError(BrevoETLError):
    """Exception raised when data processing fails."""

    pass


# ===== COMMON FUNCTIONS =====


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def get_api_configuration(audience: str):
    """Get API key and table name based on audience."""
    if audience == "native":
        api_key = access_secret_data(
            GCP_PROJECT, f"sendinblue-api-key-{ENV_SHORT_NAME}"
        )
        table_name = "brevo_newsletters"
    elif audience == "pro":
        api_key = access_secret_data(
            GCP_PROJECT, f"sendinblue-pro-api-key-{ENV_SHORT_NAME}"
        )
        table_name = "brevo_pro_newsletters"
    else:
        raise ValueError("Invalid audience. Must be one of native/pro.")

    return api_key, table_name


# ===== TRANSFORMATION FUNCTIONS =====


def transform_campaigns_to_dataframe(
    campaigns: List[Dict[str, Any]], audience: str, update_date: datetime = None
) -> pd.DataFrame:
    """
    Transform raw campaign data from Brevo API to DataFrame format.
    Matches the legacy BrevoNewsletters.get_data() transformation.
    """
    campaign_stats = {
        "campaign_id": [],
        "campaign_utm": [],
        "campaign_name": [],
        "campaign_sent_date": [],
        "share_link": [],
        "audience_size": [],
        "unsubscriptions": [],
        "open_number": [],
    }

    for camp in campaigns:
        campaign_stats["campaign_id"].append(camp.get("id"))
        campaign_stats["campaign_utm"].append(camp.get("tag"))
        campaign_stats["campaign_name"].append(camp.get("name"))
        campaign_stats["campaign_sent_date"].append(camp.get("sentDate"))
        campaign_stats["share_link"].append(camp.get("shareLink"))

        stats = camp.get("statistics", {})
        global_stats = stats.get("globalStats", {})

        campaign_stats["audience_size"].append(
            global_stats.get("delivered", 0) if global_stats else 0
        )
        campaign_stats["unsubscriptions"].append(
            global_stats.get("unsubscriptions", 0) if global_stats else 0
        )
        campaign_stats["open_number"].append(
            global_stats.get("uniqueViews", 0) if global_stats else 0
        )

    df = pd.DataFrame(campaign_stats)

    if update_date is None:
        df["update_date"] = pd.to_datetime("today")
    else:
        df["update_date"] = pd.to_datetime(update_date)

    df["campaign_target"] = audience

    return df[
        [
            "campaign_id",
            "campaign_utm",
            "campaign_name",
            "campaign_sent_date",
            "share_link",
            "audience_size",
            "open_number",
            "unsubscriptions",
            "update_date",
            "campaign_target",
        ]
    ]


def transform_events_to_dataframe(
    all_events: List[Dict[str, Any]], audience: str
) -> pd.DataFrame:
    """
    Transform raw event data to aggregated DataFrame format.
    """
    if not all_events:
        return pd.DataFrame(
            columns=[
                "template",
                "tag",
                "email",
                "event_date",
                "delivered_count",
                "opened_count",
                "unsubscribed_count",
                "target",
            ]
        )

    df = pd.DataFrame(all_events)

    df_grouped = (
        df.groupby(["tag", "template", "email", "event", "event_date"])
        .size()
        .reset_index(name="count")
    )

    df_pivot = pd.pivot_table(
        df_grouped,
        values="count",
        index=["tag", "template", "email", "event_date"],
        columns=["event"],
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    for event_type in ["delivered", "opened", "unsubscribed"]:
        if event_type not in df_pivot.columns:
            df_pivot[event_type] = 0

    df_pivot.rename(
        columns={
            "delivered": "delivered_count",
            "opened": "opened_count",
            "unsubscribed": "unsubscribed_count",
        },
        inplace=True,
    )

    df_pivot["target"] = audience

    columns = [
        "template",
        "tag",
        "email",
        "event_date",
        "delivered_count",
        "opened_count",
        "unsubscribed_count",
        "target",
    ]

    for col in columns:
        if col not in df_pivot.columns:
            df_pivot[col] = (
                np.nan
                if col not in ["delivered_count", "opened_count", "unsubscribed_count"]
                else 0
            )

    return df_pivot[columns]


# ===== LOADING FUNCTIONS =====


def save_to_historical(
    df: pd.DataFrame,
    schema: dict[str, str],
    project: str,
    dataset: str,
    table_name: str,
    end_date: datetime,
):
    """
    Upload the DataFrame to BigQuery using partitioned table naming conventions,
    creating the table if it doesn't exist.
    """
    client = bigquery.Client()
    date_str = end_date.strftime("%Y%m%d")

    # Fully‐qualified partitioned table identifier:
    #   project.dataset.table_histo$YYYYMMDD
    table_id = f"{project}.{dataset}.{table_name}_histo${date_str}"

    # First, ensure the base table exists so we can control its partitioning:
    base_table_id = f"{project}.{dataset}.{table_name}_histo"
    try:
        client.get_table(base_table_id)
        logger.info(f"Table {base_table_id} already exists")
    except NotFound:
        logger.info(
            f"Table {base_table_id} not found — creating with daily partitioning"
        )
        tbl = bigquery.Table(
            base_table_id,
            schema=[
                bigquery.SchemaField(name, dtype) for name, dtype in schema.items()
            ],
        )
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="update_date"
        )
        client.create_table(tbl)
        logger.info(f"Created partitioned table {base_table_id}")

    # Now configure the load job to overwrite the specific partition,
    # and to create the table if somehow it still doesn't exist.
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=[bigquery.SchemaField(name, dtype) for name, dtype in schema.items()],
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="update_date"
        ),
    )

    logger.info(f"Saving {len(df)} rows to {table_id}…")
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    logger.info("Upload completed.")


def save_transactional_to_historical(
    df: pd.DataFrame,
    schema: dict,
    project: str,
    dataset: str,
    table_name: str,
    date_suffix: datetime,
):
    """Save transactional data to BigQuery."""
    client = bigquery.Client()
    yyyymmdd = date_suffix.strftime("%Y%m%d")
    table_id = f"{project}.{dataset}.{yyyymmdd}_{table_name}_histo"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField(name, field_type)
            for name, field_type in schema.items()
        ],
    )

    logger.info(f"Saving {len(df)} rows to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Upload completed.")


# ===== SYNC PIPELINE FUNCTIONS =====


def etl_newsletter(
    connector: BrevoConnector,
    audience: str,
    table_name: str,
    start_date: datetime,
    end_date: datetime,
):
    """ETL pipeline newsletter campaigns."""
    logger.info("Fetching email campaigns...")

    all_campaigns = []
    offset = 0
    limit = 50  # API default limit

    while True:
        logger.info(f"Fetching campaigns: offset={offset}")

        resp = connector.get_email_campaigns(status="sent", limit=limit, offset=offset)

        if not resp:
            raise BrevoAPIError(
                "Failed to fetch campaigns - no response from Brevo API"
            )

        if not resp.ok:
            raise BrevoAPIError(
                f"Failed to fetch campaigns - HTTP {resp.status_code}: {resp.text}"
            )

        campaigns = resp.json().get("campaigns", [])

        if not campaigns:
            logger.info("No more campaigns in page.")
            break

        all_campaigns.extend(campaigns)
        logger.info(f"Fetched {len(campaigns)} campaigns (total: {len(all_campaigns)})")

        # If we got fewer campaigns than the limit, we've reached the end
        if len(campaigns) < limit:
            break

        offset += limit

    if not all_campaigns:
        raise BrevoDataError("No campaigns found in API response")

    logger.info(f"Total campaigns fetched: {len(all_campaigns)}")

    df = transform_campaigns_to_dataframe(all_campaigns, audience, update_date=end_date)

    save_to_historical(
        df,
        campaigns_histo_schema,
        GCP_PROJECT,
        BIGQUERY_RAW_DATASET,
        table_name,
        end_date,
    )


def etl_transactional(
    connector: BrevoConnector,
    audience: str,
    start_date: datetime,
    end_date: datetime,
):
    """ETL pipeline transactional emails."""
    logger.info("Fetching active SMTP templates...")

    templates_resp = connector.get_smtp_templates(active_only=True)

    if not templates_resp:
        raise BrevoAPIError("Failed to fetch templates - no response from Brevo API")

    # BrevoConnector uses SyncHttpClient which uses requests.Response.ok
    if not templates_resp.ok:
        raise BrevoAPIError(
            f"Failed to fetch templates - HTTP {templates_resp.status_code}: {templates_resp.text}"
        )

    templates = templates_resp.json().get("templates", [])

    if not templates:
        raise BrevoDataError("No active templates found")

    logger.info(f"Fetched {len(templates)} active templates")

    if ENV_SHORT_NAME != "prod" and len(templates) > 0:
        templates = templates[:1]

    all_events = []
    for template in templates:
        template_id = template.get("id")
        tag = template.get("tag")

        for event_type in ["delivered", "opened", "unsubscribed"]:
            offset = 0
            while True:
                logger.info(
                    f"Fetching events: template={template_id}, event={event_type}, offset={offset}"
                )

                events_resp = connector.get_email_event_report(
                    template_id=template_id,
                    event=event_type,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    offset=offset,
                )

                if not events_resp:
                    raise BrevoAPIError(
                        f"Failed to fetch events for template {template_id} - no response from API"
                    )

                # BrevoConnector uses SyncHttpClient which uses requests.Response.ok
                if not events_resp.ok:
                    raise BrevoAPIError(
                        f"Failed to fetch events for template {template_id} - HTTP {events_resp.status_code}: {events_resp.text}"
                    )

                events = events_resp.json().get("events", [])
                if not events:
                    logger.info("No more events in page.")
                    break

                for event in events:
                    all_events.append(
                        {
                            "template": template_id,
                            "tag": tag,
                            "email": event.get("email"),
                            "event": event.get("event"),
                            "event_date": pd.to_datetime(event.get("date")).date(),
                        }
                    )

                if len(events) < 2500:
                    break

                offset += 2500

                if ENV_SHORT_NAME != "prod":
                    break

    if not all_events:
        raise BrevoDataError("No events collected from any template")

    logger.info(f"Collected {len(all_events)} total events")

    df_final = transform_events_to_dataframe(all_events, audience)

    save_transactional_to_historical(
        df_final,
        transactional_histo_schema,
        GCP_PROJECT,
        BIGQUERY_TMP_DATASET,
        TRANSACTIONAL_TABLE_NAME,
        datetime.today(),
    )


# ===== ASYNC PIPELINE FUNCTIONS =====


async def async_etl_transactional(
    connector: AsyncBrevoConnector,
    audience: str,
    start_date: datetime,
    end_date: datetime,
):
    """Async ETL pipeline for transactional emails with concurrent template processing."""
    logger.info("[Async] Fetching active SMTP templates...")

    templates_resp = await connector.get_smtp_templates(active_only=True)

    if not templates_resp:
        raise BrevoAPIError(
            "[Async] Failed to fetch templates - no response from Brevo API"
        )

    # FIXED: Use is_success for httpx.Response instead of ok
    if not templates_resp.is_success:
        raise BrevoAPIError(
            f"[Async] Failed to fetch templates - HTTP {templates_resp.status_code}: {templates_resp.text}"
        )

    templates = templates_resp.json().get("templates", [])

    if not templates:
        raise BrevoDataError("[Async] No active templates found")

    logger.info(f"[Async] Fetched {len(templates)} active templates")

    # Limit templates in non-prod environments for testing
    if ENV_SHORT_NAME != "prod":
        logger.info(
            f"[Async] Non-prod environment ({ENV_SHORT_NAME}), limiting to 1 template for testing"
        )
        templates = templates[:1]
        logger.info(f"[Async] Processing {len(templates)} template(s)")

    # Process templates concurrently
    all_events = []
    tasks = []

    for template in templates:
        task = _fetch_template_events(connector, template, start_date, end_date)
        tasks.append(task)

    # Process templates in batches to avoid overwhelming the API
    batch_size = 3  # Process 3 templates concurrently
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i : i + batch_size]
        batch_results = await asyncio.gather(*batch)

        for events in batch_results:
            all_events.extend(events)

    if not all_events:
        raise BrevoDataError("[Async] No events collected from any template")

    logger.info(f"[Async] Collected {len(all_events)} total events")

    # Transform and save (same as sync version)
    df_final = transform_events_to_dataframe(all_events, audience)

    save_transactional_to_historical(
        df_final,
        transactional_histo_schema,
        GCP_PROJECT,
        BIGQUERY_TMP_DATASET,
        TRANSACTIONAL_TABLE_NAME,
        datetime.today(),
    )


async def _fetch_template_events(
    connector: AsyncBrevoConnector,
    template: dict,
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """Fetch all events for a single template."""
    template_id = template.get("id")
    tag = template.get("tag")
    events_list = []

    # Process event types sequentially for this template
    for event_type in ["delivered", "opened", "unsubscribed"]:
        offset = 0
        while True:
            logger.info(
                f"[Async] Fetching events: template={template_id}, "
                f"event={event_type}, offset={offset}"
            )

            # The rate limiter will handle request spacing
            try:
                events_resp = await connector.get_email_event_report(
                    template_id=template_id,
                    event=event_type,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    offset=offset,
                )

                if not events_resp:
                    raise BrevoAPIError(
                        f"[Async] Failed to fetch events for template {template_id} - no response from API"
                    )

                # FIXED: Use is_success for httpx.Response
                if not events_resp.is_success:
                    raise BrevoAPIError(
                        f"[Async] Failed to fetch events for template {template_id} - HTTP {events_resp.status_code}: {events_resp.text}"
                    )

                events = events_resp.json().get("events", [])
                if not events:
                    logger.info(f"[Async] No more events for template {template_id}")
                    break

                for event in events:
                    events_list.append(
                        {
                            "template": template_id,
                            "tag": tag,
                            "email": event.get("email"),
                            "event": event.get("event"),
                            "event_date": pd.to_datetime(event.get("date")).date(),
                        }
                    )

                if len(events) < 2500:
                    break

                offset += 2500

                if ENV_SHORT_NAME != "prod":
                    break

            except BrevoAPIError:
                # Re-raise API errors
                raise
            except Exception as e:
                raise BrevoAPIError(
                    f"[Async] Unexpected error fetching events for template {template_id}: {e}"
                ) from e

    return events_list
