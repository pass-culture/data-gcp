import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

BIGQUERY_ANALYTICS_DATASET = os.environ.get("BIGQUERY_ANALYTICS_DATASET")
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


JOUVE_API_DOMAIN = access_secret_data(GCP_PROJECT, "jouve-api-domain")
JOUVE_API_USERNAME = access_secret_data(GCP_PROJECT, "jouve-api-username")
JOUVE_API_PASSWORD = access_secret_data(GCP_PROJECT, "jouve-api-password")
JOUVE_API_VAULT_GUID = access_secret_data(GCP_PROJECT, "jouve-api-vault-guid")
