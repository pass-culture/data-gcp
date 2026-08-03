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
RAW_METABASE_DATASET = f"raw_{ENVIRONMENT_SHORT_NAME}"
METABASE_API_USERNAME = "metabase-data-bot@passculture.app"

CONFIG_DIR = Path(__file__).parent.parent / "config"

ENV_TO_CONFIG = {
    "dev": "staging.yaml",
    "stg": "staging.yaml",
    "prod": "production.yaml",
}
# Backwards-compatible alias (kept for existing imports).
ARCHIVING_ENV_TO_CONFIG = ENV_TO_CONFIG


def _get_config_path(feature):
    """Resolve a config/<feature>/<env>.yaml path based on ENV_SHORT_NAME."""
    config_file = ENV_TO_CONFIG.get(ENVIRONMENT_SHORT_NAME)
    if not config_file:
        raise ValueError(
            f"No {feature} config for environment '{ENVIRONMENT_SHORT_NAME}'"
        )
    return CONFIG_DIR / feature / config_file


def get_archiving_config_path():
    """Resolve the archiving config file path based on ENV_SHORT_NAME."""
    return _get_config_path("archiving")


def load_archiving_config():
    with open(get_archiving_config_path()) as f:
        return yaml.safe_load(f)


def get_taxonomy_config_path():
    """Resolve the taxonomy config file path based on ENV_SHORT_NAME."""
    return _get_config_path("taxonomy")


def load_taxonomy_config():
    with open(get_taxonomy_config_path()) as f:
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
