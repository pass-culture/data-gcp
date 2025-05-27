import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN = access_secret_data(
    os.environ.get("GCP_PROJECT_ID"),
    "slack-composer-dbt-test-webhook-token",
    default=None,
)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
