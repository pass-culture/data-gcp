import os
from sentence_transformers import SentenceTransformer
from pcvalidation.utils.tools import access_secret

#Project vars
GCS_BUCKET = os.environ.get("GCS_BUCKET", "data-bucket-dev")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "passculture-data-ehp")

UNUSED_COLS = ["outing", "physical_goods"]

#API
API_SECRET_KET_SECRET_ID = os.environ.get(
    "API_SECRET_KET_SECRET_ID", "api_validation_secret_key"
)
SECRET_KEY = access_secret(
    GCP_PROJECT, API_SECRET_KET_SECRET_ID, version_id="latest", default=None
)
HASH_ALGORITHM = os.environ.get("VALIDATION_LOGIN_KEY", "HS256")
LOGIN_TOKEN_EXPIRATION = os.environ.get("LOGIN_TOKEN_EXPIRATION", 30)

API_USER = os.environ.get("API_USER", "testuser")
API_PWD_SECRET_ID = os.environ.get("API_PWD_SECRET_ID", "api_validation_pwd")
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

#MLFlow
MLFLOW_SECRET_ID=os.environ.get("MLFLOW_SECRET_ID", "mlflow_client_id")
MLFLOW_CLIENT_ID=access_secret(
    GCP_PROJECT, MLFLOW_SECRET_ID, version_id="latest", default=None
)
MLFLOW_URL=os.environ.get("MLFLOW_URL", "https://mlflow.staging.passculture.team/")

#Model metadata
MODEL_DEFAULT=os.environ.get("MLFLOW_SECRET_ID", "validation_model_test")
MODEL_STAGE=os.environ.get("MODEL_STAGE", "Staging")
