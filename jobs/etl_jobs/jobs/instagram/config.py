import os

from google.cloud import bigquery

# Environment configuration
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"

# Instagram API configuration
INSTAGRAM_API_VERSION = "v23.0"
INSTAGRAM_API_RATE_LIMIT = 200  # requests per minute
MAX_ERROR_RATE = 0.2

# Instagram account IDs
INSTAGRAM_ACCOUNTS_ID = ["17841410129457081", "17841463525422101"]

# BigQuery table names
INSTAGRAM_POST_DETAIL = "instagram_post_detail"
INSTAGRAM_ACCOUNT_DAILY_ACTIVITY = "instagram_account_daily_activity"
INSTAGRAM_ACCOUNT_INSIGHTS = "instagram_account"

# BigQuery schemas
INSTAGRAM_POST_DETAIL_DTYPE = [
    bigquery.SchemaField(
        "post_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "media_type", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "caption", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "media_url", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "permalink", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "posted_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField("url_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField("shares", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField(
        "comments", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField("likes", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField("saved", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField(
        "video_views", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "total_interactions", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField("reach", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField("views", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField("follows", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField(
        "profile_visits", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "profile_activity", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "export_date", bigquery.enums.SqlTypeNames.TIMESTAMP, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "account_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
]

INSTAGRAM_ACCOUNT_DAILY_ACTIVITY_DTYPE = [
    bigquery.SchemaField(
        "event_date", bigquery.enums.SqlTypeNames.TIMESTAMP, mode="NULLABLE"
    ),
    bigquery.SchemaField("views", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
    bigquery.SchemaField("reach", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
    bigquery.SchemaField(
        "follower_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    # Deprecated metrics - kept for schema compatibility, will be set to 0
    bigquery.SchemaField(
        "email_contacts", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "phone_call_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "text_message_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "get_directions_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "website_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "profile_views", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "account_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
]

INSTAGRAM_ACCOUNT_INSIGHTS_DTYPE = [
    bigquery.SchemaField(
        "followers_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "follows_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "media_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "biography", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "username", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "export_date", bigquery.enums.SqlTypeNames.TIMESTAMP, mode="NULLABLE"
    ),
]

# Mapping of deprecated metrics for v23.0 API compatibility
DEPRECATED_METRICS_MAPPING = {
    # Deprecated profile metrics - these are no longer available in v23.0
    # We keep them in schema for compatibility but set to 0
    "deprecated_profile_metrics": [
        "email_contacts",
        "phone_call_clicks",
        "text_message_clicks",
        "get_directions_clicks",
        "website_clicks",
        "profile_views",
    ],
    # Deprecated post metrics
    "deprecated_post_metrics": [
        "video_views",  # Partially deprecated (only for carousel posts)
    ],
    # Metric replacements in v23.0
    "metric_replacements": {
        "impressions": "views",  # impressions deprecated, use views
        "plays": "views",  # plays deprecated, use views for videos
    },
}
