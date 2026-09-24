"""MLflow helpers for the item embedding job.

The MLflow server sits behind IAP, so every request needs a signed-JWT bearer
token; ``connect_to_mlflow`` mints one via the IAM Credentials API (no client
secret needed) and points the client at the environment's server.

Each step authenticates right before its short burst of logging -- the long
embedding work itself makes no MLflow calls -- so a freshly minted 1h token
never expires mid-use and no periodic refresh is needed.
"""

import datetime
import json
import os
from pathlib import Path

import google.auth
import mlflow
from google.cloud import iam_credentials_v1
from loguru import logger
from mlflow.entities import Experiment
from src.config import Vector
from src.constants import MLFLOW_RUN_ID_FILEPATH, MLFLOW_URI, SA_ACCOUNT


def _sign_jwt() -> str:
    """Sign a 1h JWT for ``SA_ACCOUNT`` via the IAM Credentials API (uses ADC)."""
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
    """Get an MLflow experiment by name, creating it (or reactivating a deleted
    one) if necessary.
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


def write_run_id(run_id: str, run_id_file: str = MLFLOW_RUN_ID_FILEPATH) -> None:
    """Write the DAG-run run_id so each later ``embed`` step can resume the run."""
    Path(run_id_file).write_text(run_id, encoding="utf-8")


def read_run_id(run_id_file: str = MLFLOW_RUN_ID_FILEPATH) -> str:
    """Read the run_id written by ``write_run_id`` in the ``mlflow_run start`` step.

    Missing file -> "" so ``embed`` still runs (provenance columns stamped empty,
    logging skipped); this keeps ``embed`` runnable standalone.
    """
    path = Path(run_id_file)
    if not path.exists():
        logger.warning(
            f"MLflow run_id file {run_id_file} not found; embeddings will not be "
            f"linked to an MLflow run and per-vector logging is skipped."
        )
        return ""
    return path.read_text(encoding="utf-8").strip()


def _flatten_config(config: dict, parent_key: str) -> dict[str, str]:
    """Flatten a nested config into dotted, ``parent_key``-prefixed param keys.

    Nested mappings recurse (``preprocessors.offer_name``); lists are joined
    (``features`` -> "a, b, c"); scalars pass through. Empty mappings contribute
    nothing. Values are stringified by ``mlflow.log_params`` downstream.
    """
    params: dict[str, str] = {}
    for key, value in config.items():
        full_key = f"{parent_key}.{key}"
        if isinstance(value, dict):
            params.update(_flatten_config(value, full_key))
        elif isinstance(value, list):
            params[full_key] = ", ".join(map(str, value))
        else:
            params[full_key] = value
    return params


def log_vector_to_run(
    run_id: str,
    vector: Vector,
    config_path: str,
    n_items_embedded: int,
    n_truncated_prompts: int,
) -> None:
    """Resume the shared DAG-run run and append one vector's config + counts.

    Logs the raw YAML as an artifact (exact source of truth) and the whole
    config flattened into vector-prefixed params (so every field, incl.
    ``prompt_template`` and ``preprocessors``, is searchable without opening the
    artifact). Keys are vector-prefixed so a single run cleanly accumulates every
    vector embedded in the DAG run. No-op when ``run_id`` is empty.
    """
    if not run_id:
        return
    connect_to_mlflow()
    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(config_path, artifact_path="configs")
        mlflow.log_params(
            _flatten_config(vector.model_dump(exclude={"name"}), vector.name)
        )
        mlflow.log_metrics(
            {
                f"{vector.name}.n_items_embedded": n_items_embedded,
                f"{vector.name}.n_truncated_prompts": n_truncated_prompts,
            }
        )
        mlflow.set_tag(f"embedded.{vector.name}", "true")
    logger.info(f"Logged vector '{vector.name}' to MLflow run {run_id}")
