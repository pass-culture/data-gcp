import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

BIGQUERY_RAW_DATASET = os.environ.get("RAW_DATASET")
ENV_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")
GCP_PROJECT = os.environ.get("PROJECT_NAME")


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


histo_schema = {
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
