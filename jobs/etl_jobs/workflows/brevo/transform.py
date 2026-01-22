from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from connectors.brevo.schemas import ApiCampaign
from workflows.brevo.schemas import TableCampaign, TableTransactionalEvent


def transform_campaigns_to_dataframe(
    campaigns: List[ApiCampaign], audience: str, update_date: datetime = None
) -> pd.DataFrame:
    """Transform validated campaign data to DataFrame format."""
    clean_campaigns = []

    current_update_date = (
        pd.to_datetime(update_date) if update_date else pd.to_datetime("today")
    )

    for camp in campaigns:
        # Map ApiCampaign (Read Schema) -> TableCampaign (Write Schema)
        clean = TableCampaign(
            campaign_id=camp.id,
            campaign_name=camp.name,
            campaign_utm=camp.tag,
            campaign_sent_date=camp.sent_date,
            share_link=camp.share_link,
            audience_size=camp.statistics.global_stats.delivered,
            unsubscriptions=camp.statistics.global_stats.unsubscriptions,
            open_number=camp.statistics.global_stats.unique_views,
            update_date=current_update_date,
            campaign_target=audience,
        )
        clean_campaigns.append(clean.model_dump())

    # DRY: Use model fields as single source of truth for columns
    cols = list(TableCampaign.model_fields.keys())

    if not clean_campaigns:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(clean_campaigns)

    return df[cols]


def transform_events_to_dataframe(
    all_events: List[Dict[str, Any]], audience: str
) -> pd.DataFrame:
    """Transform raw event data to aggregated DataFrame format."""
    # DRY: Use model fields as single source of truth for columns
    cols = list(TableTransactionalEvent.model_fields.keys())

    if not all_events:
        return pd.DataFrame(columns=cols)

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

    # Map API event types to target column names
    mapping = {
        "delivered": "delivered_count",
        "opened": "opened_count",
        "unsubscribed": "unsubscribed_count",
    }

    for api_event, target_col in mapping.items():
        if api_event in df_pivot.columns:
            df_pivot.rename(columns={api_event: target_col}, inplace=True)
        elif target_col not in df_pivot.columns:
            df_pivot[target_col] = 0

    df_pivot["target"] = audience

    # Ensure all columns exist based on schema
    for col in cols:
        if col not in df_pivot.columns:
            df_pivot[col] = 0 if "count" in col else np.nan

    return df_pivot[cols]
