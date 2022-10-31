import os
from datetime import datetime
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager, bigquery
import pandas as pd

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")
METABASE_API_USERNAME = os.environ.get("METABASE_API_USERNAME")
METABASE_HOST = os.environ.get('METABASE_HOST') # METABASE_HOST = "https://data-analytics.internal-passculture.app"
METABASE_ANALYTICS_DATASET = os.environ.get('METABASE_ANALYTICS_DATASET')

def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
