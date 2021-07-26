import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

BIGQUERY_CLEAN_DATASET = os.environ.get("BIGQUERY_CLEAN_DATASET")
ENV_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")
GCP_PROJECT = os.environ.get("GCP_PROJECT")


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


TOKEN = access_secret_data(GCP_PROJECT, "contentful-token")
SPACE_ID = access_secret_data(GCP_PROJECT, "contentful-space-id")
