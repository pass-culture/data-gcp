import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_ML_RECOMMENDATION_DATASET = f"ml_reco_{ENV_SHORT_NAME}"
BIGQUERY_ML_RETRIEVAL_DATASET = f"ml_retrieval_{ENV_SHORT_NAME}"
BIGQUERY_SEED_DATASET = f"seed_{ENV_SHORT_NAME}"
RECOMMENDATION_SQL_INSTANCE = f"cloudsql-recommendation-{ENV_SHORT_NAME}-ew1"
REGION = "europe-west1"


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
