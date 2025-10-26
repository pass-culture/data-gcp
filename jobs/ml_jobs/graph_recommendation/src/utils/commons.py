import json
import os

import mlflow
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import service_account
from loguru import logger

from src.constants import ENV_SHORT_NAME, GCP_PROJECT_ID

BUCKET_PREFIX = "gs://"
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"
MLFLOW_SECRET_NAME = "mlflow_client_id"
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)


def is_bucket_path(path: str) -> bool:
    """Check if a given path is a cloud storage bucket path."""
    return path.startswith(BUCKET_PREFIX)


def get_credentials():
    """
    Get GCP credentials with fallback strategy.

    Priority:
    1. Service account from GOOGLE_APPLICATION_CREDENTIALS env var
    2. Default credentials (gcloud auth application-default login)

    Returns:
        google.auth.credentials.Credentials: The credentials object

    Raises:
        RuntimeError: If no credentials are available
    """
    sa_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if sa_key_path:
        try:
            logger.info(f"Using service account from: {sa_key_path}")
            credentials = service_account.Credentials.from_service_account_file(
                sa_key_path
            )
            return credentials
        except Exception as e:
            logger.warning(
                f"Failed to load service account: {e}. "
                "Falling back to default credentials."
            )

    try:
        logger.info("Using default credentials (gcloud)")
        credentials, _project = default()
        return credentials
    except DefaultCredentialsError as e:
        raise RuntimeError(
            "No credentials found. Either:\n"
            "1. Set GOOGLE_APPLICATION_CREDENTIALS to your service account key path\n"
            "2. Run: gcloud auth application-default login"
        ) from e


def get_secret(secret_id: str):
    """Get secret from Secret Manager using available credentials."""
    credentials = get_credentials()
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow() -> None:
    """
    Connect to remote MLflow with authentication.

    Uses service account from Secret Manager if available,
    otherwise falls back to default credentials.
    """
    try:
        # Try to get service account from Secret Manager
        logger.info(
            "Attempting to connect to MLflow using service account from Secret Manager"
        )
        service_account_dict = json.loads(get_secret(SA_ACCOUNT))
        mlflow_client_audience = get_secret(MLFLOW_SECRET_NAME)

        id_token_credentials = (
            service_account.IDTokenCredentials.from_service_account_info(
                service_account_dict, target_audience=mlflow_client_audience
            )
        )
        id_token_credentials.refresh(Request())

        os.environ["MLFLOW_TRACKING_TOKEN"] = id_token_credentials.token
        logger.info("Successfully authenticated to MLflow with service account")

    except Exception as e:
        logger.warning(
            f"Failed to authenticate with service account from Secret Manager: {e}"
        )
        logger.info("Attempting to use default credentials for MLflow")

        try:
            # Fall back to default credentials
            credentials = get_credentials()
            mlflow_client_audience = get_secret(MLFLOW_SECRET_NAME)

            id_token_credentials = service_account.IDTokenCredentials(
                credentials, target_audience=mlflow_client_audience
            )
            id_token_credentials.refresh(Request())

            os.environ["MLFLOW_TRACKING_TOKEN"] = id_token_credentials.token
            logger.info("Successfully authenticated to MLflow with default credentials")

        except Exception as e2:
            logger.error(f"Failed to authenticate to MLflow with any method: {e2}")
            logger.warning(
                "Proceeding without MLflow authentication - tracking may not work"
            )

    mlflow.set_tracking_uri(MLFLOW_URI)


def get_mlflow_experiment(experiment_name: str):
    """Get or create MLflow experiment by name."""
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        logger.info(f"Creating new MLflow experiment: {experiment_name}")
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment
