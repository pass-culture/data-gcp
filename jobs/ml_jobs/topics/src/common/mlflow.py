"""MLflow run lifecycle and config logging.

Mirrors ``jobs/ml_jobs/item_embedding/src/mlflow_utils.py``: JWT auth via the
IAM Credentials API (no stored secret needed, since the MLflow server sits
behind IAP), one run shared across every domain command of a pipeline run,
config logged as both an artifact (exact source of truth) and flattened
params (searchable without opening the artifact).
"""

import datetime
import json
import os

import google.auth
import mlflow
from google.cloud import iam_credentials_v1
from loguru import logger
from mlflow.entities import Experiment
from pydantic import BaseModel

from src.common.constants import MLFLOW_URI, SA_ACCOUNT


def _sign_jwt() -> str:
    """Sign a 1h JWT for SA_ACCOUNT via the IAM Credentials API (uses ADC)."""
    now = datetime.datetime.now(tz=datetime.UTC)
    payload = json.dumps(
        {
            "iss": SA_ACCOUNT,
            "sub": SA_ACCOUNT,
            "aud": MLFLOW_URI + "*",
            "iat": int(now.timestamp()),
            "exp": int((now + datetime.timedelta(hours=1)).timestamp()),
        }
    )
    credentials, _ = google.auth.default()
    client = iam_credentials_v1.IAMCredentialsClient(credentials=credentials)
    name = client.service_account_path("-", SA_ACCOUNT)
    return client.sign_jwt(name=name, payload=payload).signed_jwt


def connect_to_mlflow() -> None:
    """Authenticate to the IAP-protected MLflow server and point the client at it."""
    os.environ["MLFLOW_TRACKING_TOKEN"] = _sign_jwt()
    mlflow.set_tracking_uri(MLFLOW_URI)
    logger.info(f"Connected to MLflow at {MLFLOW_URI} as {SA_ACCOUNT}")


def get_mlflow_experiment(experiment_name: str) -> Experiment:
    """Get an MLflow experiment by name, creating it (or reactivating a
    deleted one) if necessary.
    """
    client = mlflow.MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)

    if experiment is None:
        logger.info(f"Creating MLflow experiment '{experiment_name}'")
        experiment_id = client.create_experiment(name=experiment_name)
        return client.get_experiment(experiment_id)
    if experiment.lifecycle_stage == "deleted":
        logger.warning(f"Reactivating deleted MLflow experiment '{experiment_name}'")
        client.restore_experiment(experiment.experiment_id)
        return client.get_experiment(experiment.experiment_id)
    return experiment


def _config_to_params(config: dict[str, object], parent_key: str) -> dict[str, object]:
    """One ``parent_key``-prefixed param per top-level config field -- no
    flattening below that. Mappings are logged whole as a JSON param; lists
    are comma-joined; scalars pass through. Empty mappings contribute
    nothing.
    """
    params: dict[str, object] = {}
    for key, value in config.items():
        full_key = f"{parent_key}.{key}"
        if isinstance(value, dict):
            if value:
                params[full_key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, list):
            params[full_key] = ", ".join(map(str, value))
        else:
            params[full_key] = value
    return params


def log_config_to_run(
    run_id: str,
    domain: str,
    config: BaseModel,
    config_path: str,
    metrics: dict[str, float] | None = None,
) -> None:
    """Resume the shared pipeline run and append one domain's config + metrics.

    Logs the raw YAML as an artifact and one domain-prefixed param per
    top-level config field. A no-op when ``run_id`` is empty, so the calling
    command stays runnable standalone (e.g. for local testing) without a run
    having been started.
    """
    if not run_id:
        return
    connect_to_mlflow()
    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(config_path, artifact_path="configs")
        mlflow.log_params(_config_to_params(config.model_dump(), domain))
        if metrics:
            mlflow.log_metrics(
                {f"{domain}.{key}": value for key, value in metrics.items()}
            )
        mlflow.set_tag(f"{domain}.done", "true")
    logger.info(f"Logged domain '{domain}' config to MLflow run {run_id}")
