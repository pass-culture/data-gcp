import os
from sentence_transformers import SentenceTransformer
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


def access_secret(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


# Project vars
GCS_BUCKET = os.environ.get("GCS_BUCKET", "data-bucket-dev")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "passculture-data-ehp")

UNUSED_COLS = ["outing", "physical_goods"]

# API
API_SECRET_KET_SECRET_ID = os.environ.get(
    "API_SECRET_KET_SECRET_ID", "api-validation-auth-secret-key-dev"
)
SECRET_KEY = access_secret(
    GCP_PROJECT, API_SECRET_KET_SECRET_ID, version_id="latest", default=None
)
HASH_ALGORITHM = os.environ.get("VALIDATION_LOGIN_KEY", "HS256")
LOGIN_TOKEN_EXPIRATION = os.environ.get("LOGIN_TOKEN_EXPIRATION", 30)

API_USER_SECRET_ID = os.environ.get("API_USER_SECRET_ID", "api-validation-user-dev")
API_USER = access_secret(
    GCP_PROJECT, API_USER_SECRET_ID, version_id="latest", default=None
)
API_PWD_SECRET_ID = os.environ.get("API_PWD_SECRET_ID", "api-validation-password-dev")
API_PWD = access_secret(
    GCP_PROJECT, API_PWD_SECRET_ID, version_id="latest", default=None
)
users_db = {
    "testuser": {
        "username": API_USER,
        # Here the hash pswd is hashed from 'secret'
        "hashed_password": API_PWD,
        "disabled": False,
    }
}

# Encoding models
TEXT_MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
IMAGE_MODEL = SentenceTransformer("clip-ViT-B-32")

# MLFlow
MLFLOW_SECRET_ID = os.environ.get("MLFLOW_SECRET_ID", "mlflow_client_id")
MLFLOW_CLIENT_ID = access_secret(
    GCP_PROJECT, MLFLOW_SECRET_ID, version_id="latest", default=None
)
MLFLOW_URL = os.environ.get("MLFLOW_URL", "https://mlflow.staging.passculture.team/")

# Model metadata
MODEL_DEFAULT = os.environ.get("MODEL_DEFAULT", "validation_model_test")
MODEL_STAGE = os.environ.get("MODEL_STAGE", "Staging")
