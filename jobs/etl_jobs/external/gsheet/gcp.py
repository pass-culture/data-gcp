import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from loguru import logger

GCP_PROJECT_ID = os.environ["PROJECT_NAME"]
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"

SA_EXTRA_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        logger.warning("Default credentials not found, returning default value")
        return default
