import json
import os
import subprocess
import time
from datetime import datetime

import mlflow
import pandas as pd
from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import service_account

MLFLOW_EHP_URI = "https://mlflow.staging.passculture.team/"
MLFLOW_PROD_URI = "https://mlflow.passculture.team/"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"


def save_experiment(experiment_name, model_name, serving_container, run_id):
    log_results = {
        "execution_date": datetime.now().isoformat(),
        "experiment_name": experiment_name,
        "model_name": model_name,
        "model_type": "custom",
        "run_id": run_id,
        "run_start_time": int(time.time() * 1000.0),
        "run_end_time": int(time.time() * 1000.0),
        "artifact_uri": None,
        "serving_container": serving_container,
    }
    pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
        project_id=f"{GCP_PROJECT_ID}",
        if_exists="append",
    )


def deploy_container(serving_container):
    command = f"sh ./deploy_to_docker_registery.sh {serving_container}"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


def connect_remote_mlflow(client_id, env="ehp"):
    uri = MLFLOW_PROD_URI if env == "prod" else MLFLOW_EHP_URI
    service_account_dict = json.loads(get_secret(SA_ACCOUNT))
    client_id = get_secret("mlflow_client_id")
    id_token_credentials = service_account.IDTokenCredentials.from_service_account_info(
        service_account_dict, target_audience=client_id
    )
    id_token_credentials.refresh(Request())

    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token_credentials.token
    mlflow.set_tracking_uri(uri)


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment


def get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")
