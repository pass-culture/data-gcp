import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"

# Notion.
NOTION_VERSION = os.environ.get("NOTION_VERSION", "2022-06-28")
NOTION_DOCS_TABLE = "notion_dashboard_docs"


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except DefaultCredentialsError:
        return default


def get_notion_token():
    """Notion integration token from Secret Manager."""
    return access_secret_data(GCP_PROJECT, f"notion_api_key_{ENV_SHORT_NAME}")


def get_notion_db_id():
    """Dashboard-docs database id from Secret Manager."""
    return access_secret_data(
        GCP_PROJECT, f"notion_dashboard_database_id_{ENV_SHORT_NAME}"
    )
