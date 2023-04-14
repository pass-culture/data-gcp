import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")
METABASE_API_USERNAME = os.environ.get("METABASE_API_USERNAME")
METABASE_HOST = os.environ.get("METABASE_HOST")
ANALYTICS_DATASET = os.environ.get("ANALYTICS_DATASET")

parent_folder_to_archive = ["interne", "operationnel"]
limit_inactivity_in_days = 100
max_cards_to_archive = 50
sql_file = "sql/archiving_query.sql"


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
