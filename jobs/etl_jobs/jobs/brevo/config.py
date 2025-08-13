import os

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_TMP_DATASET = f"tmp_{ENV_SHORT_NAME}"

TRANSACTIONAL_TABLE_NAME = "brevo_transactional_detailed"
UPDATE_WINDOW = 31 if ENV_SHORT_NAME == "prod" else 500

campaigns_histo_schema = {
    "campaign_id": "INTEGER",
    "campaign_utm": "STRING",
    "campaign_name": "STRING",
    "campaign_target": "STRING",
    "campaign_sent_date": "STRING",
    "share_link": "STRING",
    "update_date": "DATETIME",
    "audience_size": "INTEGER",
    "open_number": "INTEGER",
    "unsubscriptions": "INTEGER",
}

transactional_histo_schema = {
    "template": "INTEGER",
    "tag": "STRING",
    "email": "STRING",
    "event_date": "DATE",
    "target": "STRING",
    "delivered_count": "INTEGER",
    "opened_count": "INTEGER",
    "unsubscribed_count": "INTEGER",
}
