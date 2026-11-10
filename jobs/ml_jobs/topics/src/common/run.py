"""The identifier threading one pipeline run through GCS model paths, BigQuery
output rows, and the MLflow run that logs its config (see
``src/common/mlflow.py``). Kept free of any MLflow import so it stays usable
by code that only cares about run identity, not experiment tracking.
"""

import pathlib
import uuid

from src.common.constants import MLFLOW_RUN_ID_FILEPATH


def new_run_id() -> str:
    """A fresh identifier for one pipeline run."""
    return uuid.uuid4().hex


def write_run_id(run_id: str, run_id_file: str = MLFLOW_RUN_ID_FILEPATH) -> None:
    """Persist the run id so later, separately-invoked CLI steps can resume it."""
    pathlib.Path(run_id_file).write_text(run_id, encoding="utf-8")


def read_run_id(run_id_file: str = MLFLOW_RUN_ID_FILEPATH) -> str:
    """Read the run id written by ``write_run_id``.

    Returns "" if the file doesn't exist, so a step can still run standalone
    (with provenance columns stamped empty) when no run was started.
    """
    path = pathlib.Path(run_id_file)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()
