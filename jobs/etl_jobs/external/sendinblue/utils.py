import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
ENV = os.environ.get("ENV")
if ENV == "dev":
    ENV = "testing"
APPLICATIVE_EXTERNAL_CONNECTION_ID = os.environ.get(
    "APPLICATIVE_EXTERNAL_CONNECTION_ID",
    f"{GCP_PROJECT}.europe-west1.metier-pcapi-{ENV}-connection",
)
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


campaigns_histo_schema = {
    "campaign_id": "INTEGER",
    "campaign_utm": "STRING",
    "campaign_name": "STRING",
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
    "user_id": "STRING",
    "event_date": "DATETIME",
    "delivered_count": "INTEGER",
    "opened_count": "INTEGER",
    "unsubscribed_count": "INTEGER",
}
