import logging
import os

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client as ClickhouseClient
from google.cloud import secretmanager

logger = logging.getLogger(__name__)

ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME", "development")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME_MAPPING = {
    "dev": "tst",
    "stg": "stg",
    "prod": "prd",
}
NEW_PROJECT_NAME = f"pc-data-{ENV_SHORT_NAME_MAPPING.get(ENV_SHORT_NAME, "dev")}"


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to access secret data: {e}")
        return default


def get_clickhouse_client(settings: dict | None = None) -> ClickhouseClient:
    return get_client(
        host=access_secret_data(PROJECT_NAME, f"data-{ENVIRONMENT_NAME}_clickhouse_ip"),
        port=access_secret_data(
            PROJECT_NAME, f"clickhouse_port_{ENV_SHORT_NAME}", default=8123
        ),
        username=access_secret_data(
            PROJECT_NAME, f"data-{ENVIRONMENT_NAME}_clickhouse_user", default="default"
        ),
        password=access_secret_data(
            PROJECT_NAME, f"data-{ENVIRONMENT_NAME}_clickhouse_password"
        ),
        settings=settings,
    )
