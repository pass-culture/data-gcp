from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd


def transform_campaigns_to_dataframe(
    campaigns: List[Dict[str, Any]], audience: str, update_date: datetime = None
) -> pd.DataFrame:
    """Transform raw campaign data from Brevo API to DataFrame format."""
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
    df["update_date"] = (
        pd.to_datetime(update_date) if update_date else pd.to_datetime("today")
    )
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
    """Transform raw event data to aggregated DataFrame format."""
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

    # Ensure all columns exist for schema mapping
    cols = [
        "template",
        "tag",
        "email",
        "event_date",
        "delivered_count",
        "opened_count",
        "unsubscribed_count",
        "target",
    ]
    for col in cols:
        if col not in df_pivot.columns:
            df_pivot[col] = 0 if "count" in col else np.nan

    return df_pivot[cols]
