import mlflow
import os
import shutil

from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import id_token

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
MLFLOW_EHP_URI = "https://mlflow-ehp.internal-passculture.app/"
MLFLOW_PROD_URI = "https://mlflow.internal-passculture.app/"
MODEL_NAME = os.environ.get("MODEL_NAME", "")
if ENV_SHORT_NAME == "prod":
    if MODEL_NAME == "v2_deep_reco":
        BOOKING_DAY_NUMBER = 1 * 30
    else:
        BOOKING_DAY_NUMBER = 4 * 30
    CLICS_DAY_NUMBER = 2 * 30
else:
    BOOKING_DAY_NUMBER = 20
    CLICS_DAY_NUMBER = 20


def get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow(client_id, env="ehp"):
    """
    Use this function to connect to the mlflow remote server.

    client_id : the oauth iap client id (in 1password)
    """
    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_PROD_URI if env == "prod" else MLFLOW_EHP_URI
    mlflow.set_tracking_uri(uri)


def remove_dir(path):
    shutil.rmtree(path)
