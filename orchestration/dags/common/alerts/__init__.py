from common.access_gcp_secrets import access_secret_data
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)

_EHP_WEBHOOK_TOKEN = access_secret_data(
    GCP_PROJECT_ID, "slack-composer-ehp-webhook-token", default=None
)
_PROD_WEBHOOK_TOKEN = access_secret_data(
    GCP_PROJECT_ID, "slack-composer-prod-webhook-token", default=None
)

SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN_DICT = {
    "dev": _EHP_WEBHOOK_TOKEN,
    "tst": _EHP_WEBHOOK_TOKEN,
    "stg": _EHP_WEBHOOK_TOKEN,
    "prod": _PROD_WEBHOOK_TOKEN,
    "prd": _PROD_WEBHOOK_TOKEN,
}

SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN = SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN_DICT[
    ENV_SHORT_NAME
]
