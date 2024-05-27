import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
import clickhouse_connect
from jinja2 import Template

BASE_DIR = "schema"


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def load_sql(dataset_name: str, table_name: str, extra_data={}, folder="tmp") -> str:
    with open(f"{BASE_DIR}/{folder}/{dataset_name}_{table_name}.sql") as file:
        sql_template = file.read()
        return Template(sql_template).render(extra_data)


def load_sql_view(view_name: str, extra_data={}, folder="analytics"):
    with open(f"{BASE_DIR}/analytics/{view_name}.sql") as file:
        sql_template = file.read()
    return Template(sql_template).render(extra_data)


def refresh_views(view_name, folder="analytics"):
    mv_view_name = f"{view_name}"
    clickhouse_client.command(
        f"DROP VIEW IF EXISTS {folder}.{view_name} ON cluster default"
    )
    sql_query = load_sql_view(view_name=view_name, folder="analytics")
    print(sql_query)
    print(f"Refresh View {mv_view_name}...")
    clickhouse_client.command(sql_query)


ENV_SHORT_NAME = {"prod": "prod", "stg": "staging", "dev": "dev"}[
    os.environ.get("ENV_SHORT_NAME", "dev")
]
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")

clickhouse_client = clickhouse_connect.get_client(
    host=access_secret_data(
        PROJECT_NAME, f"clickhouse-svc_external_ip-{ENV_SHORT_NAME}"
    ),
    port=access_secret_data(PROJECT_NAME, f"clickhouse_port_{ENV_SHORT_NAME}"),
    username=access_secret_data(PROJECT_NAME, f"clickhouse_username_{ENV_SHORT_NAME}"),
    password=access_secret_data(
        PROJECT_NAME, f"clickhouse-admin_password-{ENV_SHORT_NAME}"
    ),
)
