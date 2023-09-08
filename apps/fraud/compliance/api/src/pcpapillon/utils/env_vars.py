import os
import contextvars
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


def access_secret(project_id, secret_id, version_id="latest", default=None):
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
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
# API_LOCAL is string to match terraform boolean handling
API_LOCAL = os.environ.get("API_LOCAL", False)
isAPI_LOCAL = True if API_LOCAL == "True" else False
# API
API_SECRET_KET_SECRET_ID = os.environ.get(
    "API_SECRET_KET_SECRET_ID", "api-papillon-auth-secret-key-dev"
)
SECRET_KEY = access_secret(GCP_PROJECT, API_SECRET_KET_SECRET_ID)
HASH_ALGORITHM = os.environ.get("VALIDATION_LOGIN_KEY", "HS256")
LOGIN_TOKEN_EXPIRATION = os.environ.get("LOGIN_TOKEN_EXPIRATION", 30)

if API_LOCAL:
    API_USER = "user_local"
    API_PWD = "pwd_local"

else:
    API_USER_SECRET_ID = os.environ.get("API_USER_SECRET_ID", "api-papillon-user-dev")
    API_USER = access_secret(GCP_PROJECT, API_USER_SECRET_ID)
    API_PWD_SECRET_ID = os.environ.get("API_PWD_SECRET_ID", "api-papillon-password-dev")
    API_PWD = access_secret(GCP_PROJECT, API_PWD_SECRET_ID)
users_db = {
    API_USER: {
        "username": API_USER,
        "password": API_PWD,
        "disabled": False,
    }
}
# Configs
# logger
cloud_trace_context = contextvars.ContextVar("cloud_trace_context", default="")
http_request_context = contextvars.ContextVar("http_request_context", default=dict({}))
# MLFlow
MLFLOW_SECRET_ID = os.environ.get("MLFLOW_SECRET_ID", "mlflow_client_id")
MLFLOW_CLIENT_ID = access_secret(GCP_PROJECT, MLFLOW_SECRET_ID)
MLFLOW_URL = os.environ.get("MLFLOW_URL", "https://mlflow.staging.passculture.team/")
# Model metadata
MODEL_DEFAULT = os.environ.get("MODEL_DEFAULT", "compliance_model_dev")
MODEL_STAGE = os.environ.get("MODEL_STAGE", "Production")
