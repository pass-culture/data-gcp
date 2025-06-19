from common.access_gcp_secrets import access_secret_data
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)

SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN = {
    "prod": access_secret_data(
        GCP_PROJECT_ID, "slack-composer-prod-webhook-token", default=None
    ),
    "stg": access_secret_data(
        GCP_PROJECT_ID, "slack-composer-ehp-webhook-token", default=None
    ),
    "dev": access_secret_data(
        GCP_PROJECT_ID, "slack-composer-ehp-webhook-token", default=None
    ),
}[ENV_SHORT_NAME]
