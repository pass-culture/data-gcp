from datetime import datetime, timedelta
import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

KEY_ID = "CR65Y9UN25"
ISSUER_ID = "591abf9a-10b5-4c9e-a70b-62bf336008d4"

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENVIRONMENT_SHORT_NAME}"


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


PRIVATE_KEY = access_secret_data(PROJECT_NAME, f"api-apple-{ENVIRONMENT_SHORT_NAME}")
BUCKET_NAME = access_secret_data(
    PROJECT_NAME, f"downloads_bucket_name_{ENVIRONMENT_SHORT_NAME}"
)


def get_last_month(today):

    current_month_first_day = today.replace(day=1)
    last_month = (current_month_first_day - timedelta(days=1)).replace(day=1)
    return last_month
