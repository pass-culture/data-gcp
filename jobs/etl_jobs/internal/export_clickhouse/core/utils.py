import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
import clickhouse_connect


ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


CLICKHOUSE_CLIENT = clickhouse_connect.get_client(
    host=access_secret_data(
        PROJECT_NAME, f"clickhouse-svc_external_ip-{ENV_SHORT_NAME}"
    ),
    port=access_secret_data(
        PROJECT_NAME, f"clickhouse_port_{ENV_SHORT_NAME}", default=8123
    ),
    username=access_secret_data(
        PROJECT_NAME, f"clickhouse_username_{ENV_SHORT_NAME}", default="default"
    ),
    password=access_secret_data(
        PROJECT_NAME, f"clickhouse-admin_password-{ENV_SHORT_NAME}"
    ),
)
