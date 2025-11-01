import json
import os
import tempfile
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

import mlflow
import pandas as pd
import typer
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from loguru import logger
from mlflow.entities import Experiment

from src.utils.gcp import (
    ENV_SHORT_NAME,
    SA_ACCOUNT,
    get_credentials,
    get_secret,
)

MLFLOW_SECRET_NAME = "mlflow_client_id"
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)

# Global variable to store credentials for token refresh
_mlflow_credentials = None


@contextmanager
def optional_mlflow_logging(enabled: bool = True):  # noqa: FBT001
    """Context manager to conditionally enable/disable MLflow logging."""
    if not enabled:
        logger.warning(
            "MLflow logging is DISABLED - all mlflow.log_* calls will be no-ops"
        )

        # Include all log_* and key setup methods
        patch_methods = [
            m
            for m in dir(mlflow)
            if m.startswith("log_")
            or m in {"set_tracking_uri", "create_experiment", "get_experiment_by_name"}
        ]

        originals = {}
        for method_name in patch_methods:
            method = getattr(mlflow, method_name)
            if callable(method):
                originals[method_name] = method
                setattr(mlflow, method_name, lambda *args, **kwargs: None)

        # Run function without MLflow logging
        try:
            yield
        finally:
            # Restore original functions
            for method_name, original_method in originals.items():
                setattr(mlflow, method_name, original_method)
            logger.info("MLflow logging was disabled and is now re-enabled")
    else:
        yield


def conditional_mlflow(log_mlflow_arg_name: str = "log_mlflow"):
    """Decorator factory to conditionally enable MLflow logging.

    Simply adding a `log_mlflow` boolean argument to the decorated functions.
    If `log_mlflow` is False, all MLflow logging calls within the function
    will be no-ops."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_mlflow = kwargs.pop(log_mlflow_arg_name, True)

            with optional_mlflow_logging(log_mlflow):
                return func(*args, **kwargs)

        return wrapper

    return decorator


@conditional_mlflow()
def connect_remote_mlflow() -> None:
    """
    Connect to remote MLflow with authentication.

    Uses service account from Secret Manager if available,
    otherwise falls back to default credentials.
    """
    global _mlflow_credentials

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
        _mlflow_credentials = id_token_credentials
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
            _mlflow_credentials = id_token_credentials
            logger.info("Successfully authenticated to MLflow with default credentials")

        except Exception as e2:
            logger.error(f"Failed to authenticate to MLflow with any method: {e2}")
            logger.warning(
                "Proceeding without MLflow authentication - tracking may not work"
            )

    mlflow.set_tracking_uri(MLFLOW_URI)


@conditional_mlflow()
def refresh_mlflow_token() -> None:
    """
    Refresh the MLflow authentication token.

    Call this periodically (e.g., every epoch) in long-running training jobs
    to prevent token expiration.
    """
    global _mlflow_credentials
    if _mlflow_credentials is None:
        logger.warning("No credentials available for token refresh")
        return

    try:
        _mlflow_credentials.refresh(Request())
        os.environ["MLFLOW_TRACKING_TOKEN"] = _mlflow_credentials.token
        logger.debug("MLflow token refreshed successfully")
    except Exception as e:
        logger.error(f"Failed to refresh MLflow token: {e}")


@conditional_mlflow()
def get_mlflow_experiment(experiment_name: str) -> Experiment:
    """Get or create an MLflow experiment, ensuring it is active."""
    experiment = mlflow.get_experiment_by_name(experiment_name)

    if experiment is None:
        logger.info(f"Creating new MLflow experiment: {experiment_name}")
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    elif experiment.lifecycle_stage == "deleted":
        logger.warning(
            f"MLflow experiment '{experiment_name}' is deleted. "
            "Creating a new experiment instead."
        )
        # Optionally: reactivate instead of creating a new one
        experiment_name = f"{experiment_name}_recreated"
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)

    return experiment


@conditional_mlflow()
def log_model_parameters(params: dict) -> None:
    """
    Log model parameters to MLflow.

    Args:
        params (dict): Dictionary containing model parameters
    """
    metadata = params.copy()
    _metapath = metadata.pop("metapath")
    # Format metapath and log it
    metapath_dict = {
        "metapath": [
            {"step": i, "edge": f"{step[0]}->{step[1]}->{step[2]}"}
            for i, step in enumerate(_metapath)
        ]
    }
    mlflow.log_dict(metapath_dict, "config/metapath.json")

    # Log remaining parameters
    metadata["metapath_length"] = len(_metapath)
    mlflow.log_params(metadata)


@conditional_mlflow()
def _log_metrics_at_k_csv(
    metrics_df: pd.DataFrame, order_by: list[str] | None = None
) -> None:
    """
    Converts metrics DataFrame into tidy format and logs as csv artifact to MLflow.

    Args:
        metrics_df: DataFrame with columns like
        ['score_col', 'k', 'threshold', 'ndcg', 'recall', 'precision']

    Returns:
        tidy_df: DataFrame with columns
        ['score_col', 'k', 'threshold', '{metric}_at_k', 'value']
    """
    tidy_rows = []

    metric_cols = [
        c for c in metrics_df.columns if c not in ["score_col", "k", "threshold"]
    ]

    for _, row in metrics_df.iterrows():
        for metric_name in metric_cols:
            # Only thresholded metrics should vary per threshold
            threshold_val = (
                row["threshold"] if metric_name in ["recall", "precision"] else None
            )
            tidy_rows.append(
                {
                    "score_col": row["score_col"],
                    "k": row["k"],
                    "threshold": threshold_val,
                    "metric": f"{metric_name}_at_k",
                    "value": float(row[metric_name]),
                }
            )

    tidy_df = pd.DataFrame(tidy_rows).sort_values(by=order_by)

    # Log metrics to MLflow
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "evaluation_metrics.csv"
        tidy_df.to_csv(local_path, index=False)
        mlflow.log_artifact(str(local_path), artifact_path=None)


@conditional_mlflow()
def log_evaluation_metrics(
    metrics_df: pd.DataFrame, output_metrics_path: str, *, store_csv: bool = True
) -> None:
    # Log metrics at k with k as x-axis
    # Log threshold-dependent metrics (recall and precision)
    for _, row in metrics_df.iterrows():
        metric_suffix = f"thresh_{row['threshold']}__{row['score_col']}"
        mlflow.log_metric(
            f"recall_at_{str(row['k']).zfill(3)}__{metric_suffix}", row["recall"]
        )
        mlflow.log_metric(
            f"curve_recall_at_k__{metric_suffix}", row["recall"], step=int(row["k"])
        )
        mlflow.log_metric(
            f"precision_at_{str(row['k']).zfill(3)}__{metric_suffix}",
            row["precision"],
        )
        mlflow.log_metric(
            f"curve_precision_at_k__{metric_suffix}",
            row["precision"],
            step=int(row["k"]),
        )

    # Log NDCG
    ndcg_metrics = metrics_df.drop_duplicates(subset=["score_col", "k"])
    for _, row in ndcg_metrics.iterrows():
        mlflow.log_metric(
            f"ndcg_at_{str(row['k']).zfill(3)}__{row['score_col']}",
            row["ndcg"],
        )
        mlflow.log_metric(
            f"curve_ndcg_at_k__{row['score_col']}",
            row["ndcg"],
            step=int(row["k"]),
        )

    # Log metrics artifact for easy lookup
    _log_metrics_at_k_csv(
        metrics_df, order_by=["score_col", "threshold", "metric", "k"]
    )

    typer.secho(
        f"Metrics saved to: {output_metrics_path}",
        fg=typer.colors.GREEN,
        err=True,
    )
    if store_csv:
        # Save detailed scores locally first (mandatory for csv)
        filename = output_metrics_path.split("/")[-1]
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / filename
            metrics_df.to_csv(local_path, index=False)

            # Then log as MLflow artifact
            mlflow.log_artifact(str(local_path), artifact_path=None)


@conditional_mlflow()
def log_detailed_scores(
    results_df: pd.DataFrame, output_detailed_scores_path: str | None
) -> None:
    # Save detailed scores if requested
    if output_detailed_scores_path:
        results_df.to_parquet(output_detailed_scores_path, index=False)
        typer.secho(
            f"Detailed query scores saved to: {output_detailed_scores_path}",
            fg=typer.colors.GREEN,
            err=True,
        )


@conditional_mlflow()
def log_graph_analysis(
    graph_summary: pd.DataFrame, graph_components: pd.DataFrame
) -> None:
    """Log graph analysis statistics to MLflow."""

    with tempfile.TemporaryDirectory() as tmpdir:
        local_graph_path = Path(tmpdir) / "graph_components.csv"
        graph_components.to_csv(local_graph_path, index=False)
        mlflow.log_artifact(str(local_graph_path), artifact_path=None)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_graph_path = Path(tmpdir) / "graph_summary.csv"
        graph_summary.to_csv(local_graph_path, index=False)
        mlflow.log_artifact(str(local_graph_path), artifact_path=None)
