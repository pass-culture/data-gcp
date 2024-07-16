import os
from typing import Literal

import mlflow
from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import id_token
from mlflow.entities import Experiment

from utils.constants import (
    GCP_PROJECT_ID,
    MLFLOW_EHP_URI,
    MLFLOW_PROD_URI,
    MLFLOW_SECRET_NAME,
)


def get_mlflow_client_id() -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{MLFLOW_SECRET_NAME}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow(
    client_id: str, mlflow_env: Literal["ehp", "prod"] = "ehp"
) -> None:
    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_PROD_URI if mlflow_env == "prod" else MLFLOW_EHP_URI
    mlflow.set_tracking_uri(uri)


def get_mlflow_experiment(experiment_name: str) -> Experiment:
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment
