import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")

NOTION_GLOSSARY_SECRET_NAME = f"notion_glossary_database_id_{ENVIRONMENT_SHORT_NAME}"
NOTION_DOCUMENTATION_SECRET_NAME = (
    f"notion_documentation_database_id_{ENVIRONMENT_SHORT_NAME}"
)
NOTION_API_KEY_SECRET_NAME = f"notion_api_key_{ENVIRONMENT_SHORT_NAME}"
RAW_DATASET = f"raw_{ENVIRONMENT_SHORT_NAME}"


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
