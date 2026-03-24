"""MLflow integration utilities for experiment tracking.

This module provides helper functions for setting up MLflow tracking with
Google Cloud authentication and experiment management.
"""

import datetime
import json
import os

import mlflow
import mlflow.entities
from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import service_account

from forecast.utils.constants import (
    GCP_PROJECT_ID,
    MLFLOW_SECRET_NAME,
    MLFLOW_URI,
    SA_ACCOUNT,
)


def get_secret(secret_id: str) -> str:
    """Retrieve a secret from Google Cloud Secret Manager.

    Args:
        secret_id: ID of the secret to retrieve.

    Returns:
        The secret value as a string.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow() -> None:
    """Configure MLflow to connect to remote tracking server.

    Sets up authentication using service account credentials from Secret Manager
    and configures the MLflow tracking URI.
    """
    service_account_dict = json.loads(get_secret(SA_ACCOUNT))
    mlflow_client_audience = get_secret(MLFLOW_SECRET_NAME)

    id_token_credentials = service_account.IDTokenCredentials.from_service_account_info(
        service_account_dict, target_audience=mlflow_client_audience
    )
    id_token_credentials.refresh(Request())

    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token_credentials.token
    mlflow.set_tracking_uri(MLFLOW_URI)


def get_mlflow_experiment(experiment_name: str) -> mlflow.entities.Experiment:
    """Get or create an MLflow experiment.

    Args:
        experiment_name: Name of the experiment.

    Returns:
        MLflow experiment object.
    """
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment


def setup_mlflow(experiment_name: str, model_type: str, model_name: str):
    """Set up MLflow tracking for a training run.

    Connects to remote MLflow server, gets or creates experiment,
    and generates a unique run name.

    Args:
        experiment_name: Name of the MLflow experiment.
        model_type: Type of the model being trained.
        model_name: Name of the model being trained.

    Returns:
        Tuple of (experiment, run_name).
    """
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name)
    run_name = f"{model_type}_{model_name}_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    return experiment, run_name
