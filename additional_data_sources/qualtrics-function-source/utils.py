import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


PROJECT_NAME = os.environ.get("PROJECT_NAME")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


API_TOKEN = access_secret_data(
    PROJECT_NAME, f"qualtrics_token_{ENVIRONMENT_SHORT_NAME}"
)
DATA_CENTER = access_secret_data(
    PROJECT_NAME, f"qualtrics_data_center_{ENVIRONMENT_SHORT_NAME}"
)
DIRECTORY_ID = access_secret_data(
    PROJECT_NAME, f"qualtrics_directory_id_{ENVIRONMENT_SHORT_NAME}"
)
