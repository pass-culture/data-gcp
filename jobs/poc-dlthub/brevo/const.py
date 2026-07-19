import os
from datetime import datetime, timezone

AUDIENCES = ["native", "pro"]
BREVO_BASE_URL = os.getenv("BREVO_BASE_URL", "https://api.brevo.com/v3/")
GCP_PROJECT = os.getenv("GCP_PROJECT", "no-project-defined")
TARGET_ENV = os.environ.get("TARGET_ENV", "ehp")

# Brevo requires endDate when startDate is provided
_NOW_ISO = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

ENDPOINTS = [
    # 1. Newsletter Campaigns
    {
        "name": "email_campaigns",
        "endpoint": {
            "path": "emailCampaigns",
            "params": {
                "status": "sent",
                "statistics": "globalStats",
            },
            "data_selector": "campaigns",
            "paginator": {
                "type": "offset",
                "limit": 50,
                "total_path": "count",
            },
            "incremental": {
                "cursor_path": "sentDate",
                "start_param": "startDate",
                "end_param": "endDate",
                "initial_value": "2024-01-01T00:00:00.000Z",
                "end_value": _NOW_ISO,
                "on_cursor_value_missing": "exclude",
            },
        },
    },
    # 2. SMTP Templates (used as parent for transactional events)
    {
        "name": "smtp_templates",
        "endpoint": {
            "path": "smtp/templates",
            "params": {
                "templateStatus": "true",
            },
            "data_selector": "templates",
            "paginator": {
                "type": "offset",
                "limit": 50,
                "total_path": "count",
            },
        },
    },
    # 3. Transactional Email Events — one child resource per event type
    #    Raw events, no aggregation
    {
        "name": "transactional_events_delivered",
        "table_name": "transactional_events",
        "endpoint": {
            "path": "smtp/statistics/events",
            "params": {
                "event": "delivered",
                "templateId": "{resources.smtp_templates.id}",
            },
            "data_selector": "events",
            "paginator": {
                "type": "offset",
                "limit": 2500,
                "total_path": None,
            },
        },
        "include_from_parent": ["id", "tag"],
        "primary_key": None,
        "write_disposition": "append",
    },
    {
        "name": "transactional_events_opened",
        "table_name": "transactional_events",
        "endpoint": {
            "path": "smtp/statistics/events",
            "params": {
                "event": "opened",
                "templateId": "{resources.smtp_templates.id}",
            },
            "data_selector": "events",
            "paginator": {
                "type": "offset",
                "limit": 2500,
                "total_path": None,
            },
        },
        "include_from_parent": ["id", "tag"],
        "primary_key": None,
        "write_disposition": "append",
    },
    {
        "name": "transactional_events_unsubscribed",
        "table_name": "transactional_events",
        "endpoint": {
            "path": "smtp/statistics/events",
            "params": {
                "event": "unsubscribed",
                "templateId": "{resources.smtp_templates.id}",
            },
            "data_selector": "events",
            "paginator": {
                "type": "offset",
                "limit": 2500,
                "total_path": None,
            },
        },
        "include_from_parent": ["id", "tag"],
        "primary_key": None,
        "write_disposition": "append",
    },
]
