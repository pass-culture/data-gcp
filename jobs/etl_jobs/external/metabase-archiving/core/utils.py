import os
from pathlib import Path

import yaml
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
ENVIRONMENT_LONG_NAME = {
    "dev": "development",
    "stg": "staging",
    "prod": "production",
}[ENVIRONMENT_SHORT_NAME]

INT_METABASE_DATASET = f"int_metabase_{ENVIRONMENT_SHORT_NAME}"
METABASE_API_USERNAME = "metabase-data-bot@passculture.app"

CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_archiving_config():
    with open(CONFIG_DIR / "archiving.yaml") as f:
        return yaml.safe_load(f)


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


METABASE_HOST = access_secret_data(
    PROJECT_NAME, f"metabase_host_{ENVIRONMENT_LONG_NAME}"
)

CLIENT_ID = access_secret_data(
    PROJECT_NAME, f"metabase-{ENVIRONMENT_LONG_NAME}_oauth2_client_id"
)

PASSWORD = access_secret_data(
    PROJECT_NAME, f"metabase-api-secret-{ENVIRONMENT_SHORT_NAME}"
)
