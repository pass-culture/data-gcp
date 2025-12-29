import json
import os

import mlflow
from constants import GCP_PROJECT_ID, MLFLOW_SECRET_NAME, MLFLOW_URI, SA_ACCOUNT
from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import service_account

MLFLOW_EHP_URI = "https://mlflow.staging.passculture.team/"
MLFLOW_PROD_URI = "https://mlflow.passculture.team/"


def get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow() -> None:
    service_account_dict = json.loads(get_secret(SA_ACCOUNT))
    mlflow_client_audience = get_secret(MLFLOW_SECRET_NAME)

    id_token_credentials = service_account.IDTokenCredentials.from_service_account_info(
        service_account_dict, target_audience=mlflow_client_audience
    )
    id_token_credentials.refresh(Request())

    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token_credentials.token
    mlflow.set_tracking_uri(MLFLOW_URI)


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment
