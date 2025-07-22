import time
import logging
from functools import wraps
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import numpy as np
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager
from http_custom.rate_limiters import BaseRateLimiter
from connectors.brevo import BrevoConnector
from jobs.brevo.config import (
    GCP_PROJECT,
    ENV_SHORT_NAME,
    TRANSACTIONAL_TABLE_NAME,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
    campaigns_histo_schema,
    transactional_histo_schema,
)

logger = logging.getLogger(__name__)

# ===== CUSTOM RATE LIMITER =====

class SyncBrevoHeaderRateLimiter(BaseRateLimiter):
    """
    Brevo-specific rate limiter using headers:
    - x-sib-ratelimit-limit
    - x-sib-ratelimit-remaining
    - x-sib-ratelimit-reset
    """
    def acquire(self):
        # No pre-request throttling — Brevo rate is dynamic
        pass

    def backoff(self, response):
        try:
            reset = float(response.headers.get("x-sib-ratelimit-reset", "10"))
            logger.warning(f"Rate limited. Waiting {reset:.2f}s based on x-sib-ratelimit-reset header...")
            time.sleep(reset)
        except Exception:
            logger.warning("Fallback backoff: 10s")
            time.sleep(10)

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
        table_name = "sendinblue_newsletters"
    elif audience == "pro":
        api_key = access_secret_data(
            GCP_PROJECT, f"sendinblue-pro-api-key-{ENV_SHORT_NAME}"
        )
        table_name = "sendinblue_pro_newsletters"
    else:
        raise ValueError("Invalid audience. Must be one of native/pro.")
    
    return api_key, table_name

# ===== TRANSFORMATION FUNCTIONS =====

def transform_campaigns_to_dataframe(campaigns: List[Dict[str, Any]], audience: str, update_date: datetime = None) -> pd.DataFrame:
    """
    Transform raw campaign data from Brevo API to DataFrame format.
    Matches the legacy BrevoNewsletters.get_data() transformation.
    
    Args:
        campaigns: List of campaign dictionaries from API response
        audience: Target audience ('native' or 'pro')
        update_date: Date to use for update_date field (defaults to today)
    
    Returns:
        pd.DataFrame with transformed campaign data
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
        
        # Handle statistics with same logic as legacy code
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
    
    # Create DataFrame with same structure as legacy code
    df = pd.DataFrame(campaign_stats)
    
    # Use provided update_date or default to today
    if update_date is None:
        df["update_date"] = pd.to_datetime("today")
    else:
        # Ensure update_date matches the partition date
        df["update_date"] = pd.to_datetime(update_date)
    
    df["campaign_target"] = audience
    
    # Ensure column order matches legacy
    return df[[
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
    ]]


def transform_events_to_dataframe(all_events: List[Dict[str, Any]], audience: str) -> pd.DataFrame:
    """
    Transform raw event data to aggregated DataFrame format.
    Matches the legacy BrevoTransactional.parse_to_df() transformation.
    
    Args:
        all_events: List of event dictionaries
        audience: Target audience ('native' or 'pro')
    
    Returns:
        pd.DataFrame with aggregated event data
    """
    if not all_events:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=[
            "template", "tag", "email", "event_date", 
            "delivered_count", "opened_count", "unsubscribed_count", "target"
        ])
    
    # Create initial DataFrame
    df = pd.DataFrame(all_events)
    
    # Group and count events (matching legacy logic)
    df_grouped = df.groupby(
        ["tag", "template", "email", "event", "event_date"]
    ).size().reset_index(name="count")
    
    # Pivot to get counts by event type
    df_pivot = pd.pivot_table(
        df_grouped,
        values="count",
        index=["tag", "template", "email", "event_date"],
        columns=["event"],
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    
    # Ensure all event columns exist
    for event_type in ["delivered", "opened", "unsubscribed"]:
        if event_type not in df_pivot.columns:
            df_pivot[event_type] = 0
    
    # Rename columns to match schema
    df_pivot.rename(columns={
        "delivered": "delivered_count",
        "opened": "opened_count",
        "unsubscribed": "unsubscribed_count",
    }, inplace=True)
    
    # Add target column
    df_pivot["target"] = audience
    
    # Ensure column order and types match schema
    columns = [
        "template", "tag", "email", "event_date",
        "delivered_count", "opened_count", "unsubscribed_count", "target"
    ]
    
    # Ensure all columns exist
    for col in columns:
        if col not in df_pivot.columns:
            df_pivot[col] = np.nan if col not in ["delivered_count", "opened_count", "unsubscribed_count"] else 0
    
    return df_pivot[columns]

# ===== LOADING FUNCTIONS =====

def save_to_historical(
    df: pd.DataFrame,
    schema: dict,
    project: str,
    dataset: str,
    table_name: str,
    end_date: datetime,
):
    """
    Upload the DataFrame to BigQuery using partitioned table naming conventions.

    Args:
        df (pd.DataFrame): DataFrame to write
        schema (dict): BigQuery schema, e.g. {"field": "STRING"}
        project (str): GCP project name
        dataset (str): BigQuery dataset
        table_name (str): Base table name
        end_date (datetime): Date used for partitioning
    """
    client = bigquery.Client()
    yyyymmdd = end_date.strftime("%Y%m%d")
    table_id = f"{project}.{dataset}.{table_name}_histo${yyyymmdd}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, 
            field="update_date"
        ),
        schema=[
            bigquery.SchemaField(name, field_type)
            for name, field_type in schema.items()
        ],
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
    )

    logger.info(f"Saving {len(df)} rows to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Upload completed.")


def save_transactional_to_historical(
    df: pd.DataFrame,
    schema: dict,
    project: str,
    dataset: str,
    table_name: str,
    date_suffix: datetime,
):
    """
    Save transactional data to BigQuery.
    Uses different naming convention than newsletter data.
    """
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

# ===== PIPELINES FUNCTIONS =====

def etl_newsletter(
    connector: BrevoConnector,
    audience: str,
    table_name: str,
    start_date: datetime,
    end_date: datetime,
):
    """ETL pipeline newsletter campaigns."""
    logger.info("Fetching email campaigns...")
    
    # Fetch campaigns - legacy code doesn't use date filtering for campaigns API
    # The dates are only used for BigQuery partitioning
    resp = connector.get_email_campaigns()
    
    if not resp:
        logger.warning("No campaigns fetched.")
        return
    
    campaigns = resp.json().get("campaigns", [])
    logger.info(f"Fetched {len(campaigns)} campaigns")
    
    if not campaigns:
        logger.warning("No campaigns in response.")
        return
    
    # Note: Legacy code doesn't paginate campaigns, only fetches first batch
    # If you need all campaigns, you would need to implement pagination here
    
    # Transform campaigns using the utility function
    # Pass end_date to ensure update_date matches the partition
    df = transform_campaigns_to_dataframe(campaigns, audience, update_date=end_date)
    
    # Save to BigQuery using the same logic as legacy code
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
    
    # Fetch templates
    templates_resp = connector.get_smtp_templates(active_only=True)
    templates = templates_resp.json().get("templates", []) if templates_resp else []
    
    if not templates:
        logger.warning("No active templates found.")
        return
    
    logger.info(f"Fetched {len(templates)} active templates")
    
    # Limit templates in non-prod (matching legacy behavior)
    if ENV_SHORT_NAME != "prod" and len(templates) > 0:
        templates = templates[:1]
    
    # Collect all events
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
                    logger.warning("No response received. Skipping.")
                    break
                
                events = events_resp.json().get("events", [])
                if not events:
                    logger.info("No more events in page.")
                    break
                
                # Collect events with same structure as legacy
                for event in events:
                    all_events.append({
                        "template": template_id,
                        "tag": tag,
                        "email": event.get("email"),
                        "event": event.get("event"),
                        "event_date": pd.to_datetime(event.get("date")).date(),
                    })
                
                # Check if we need to paginate (matching legacy logic)
                if len(events) < 2500:
                    break
                    
                offset += 2500
                
                # In non-prod, don't paginate beyond first page
                if ENV_SHORT_NAME != "prod":
                    break
    
    if not all_events:
        logger.warning("No events collected.")
        return
    
    logger.info(f"Collected {len(all_events)} total events")
    
    # Transform events using utility function
    df_final = transform_events_to_dataframe(all_events, audience)
    
    # Save to BigQuery
    save_transactional_to_historical(
        df_final,
        transactional_histo_schema,
        GCP_PROJECT,
        BIGQUERY_TMP_DATASET,
        TRANSACTIONAL_TABLE_NAME,
        datetime.today(),  # Using today's date for file naming
    )