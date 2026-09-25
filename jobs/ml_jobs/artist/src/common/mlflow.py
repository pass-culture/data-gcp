import json
import os
from typing import TYPE_CHECKING

from google.auth.transport.requests import Request
from google.oauth2 import service_account

from src.common.constants import (
    MLFLOW_SECRET_NAME,
    MLFLOW_URI,
    SA_ACCOUNT,
)
from src.common.gcp import get_secret

if TYPE_CHECKING:
    from mlflow.entities import Experiment

# `mlflow` itself is imported lazily inside each function, not at module level:
# it's only an optional dependency of the linkage domain (see pyproject.toml's
# `linkage` extra), and this module lives in common/ so it must stay importable
# — e.g. for extraction, which never installs mlflow — without requiring the
# package to be present until one of these functions is actually called.


def connect_remote_mlflow() -> None:
    import mlflow

    service_account_dict = json.loads(get_secret(SA_ACCOUNT))
    mlflow_client_audience = get_secret(MLFLOW_SECRET_NAME)

    id_token_credentials = service_account.IDTokenCredentials.from_service_account_info(
        service_account_dict, target_audience=mlflow_client_audience
    )
    id_token_credentials.refresh(Request())

    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token_credentials.token
    mlflow.set_tracking_uri(MLFLOW_URI)


def get_mlflow_experiment(experiment_name: str) -> "Experiment":
    import mlflow

    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment
