import os

import mlflow
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from utils.constants import MLFLOW_EHP_URI, MLFLOW_PROD_URI


def connect_remote_mlflow(client_id, env="ehp"):
    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_PROD_URI if env == "prod" else MLFLOW_EHP_URI
    mlflow.set_tracking_uri(uri)


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment
