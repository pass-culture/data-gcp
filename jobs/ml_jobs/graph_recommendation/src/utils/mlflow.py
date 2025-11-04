import json
import os
import tempfile
import time
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

import mlflow
import pandas as pd
import typer
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from loguru import logger

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
MLFLOW_TOKEN_REFRESH_INTERVAL = 300


def _get_mlflow_log_functions_to_patch(extra_functions: set | None = None):
    patch_prefix = "log_"
    if not extra_functions:
        extra_functions = {}
    return [
        m for m in dir(mlflow) if m.startswith(patch_prefix) or m in extra_functions
    ]


@contextmanager
def optional_mlflow_logging(enabled: bool = True):  # noqa: FBT001
    """Context manager to conditionally enable/disable MLflow logging."""
    if not enabled:
        logger.warning(
            "MLflow logging is DISABLED - all mlflow.log_* calls will be no-ops"
        )

        # Include all log_* and key setup methods
        patch_methods = _get_mlflow_log_functions_to_patch(
            extra_functions={"create_experiment", "get_experiment_by_name"}
        )

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


class MLflowAuthManager:
    """Handles MLflow authentication and periodic token refresh."""

    def __init__(
        self,
        mlflow_uri: str = MLFLOW_URI,
        sa_secret_name: str = SA_ACCOUNT,
        client_secret_name: str = MLFLOW_SECRET_NAME,
        token_refresh_interval: int = MLFLOW_TOKEN_REFRESH_INTERVAL,
    ):
        self.mlflow_uri = mlflow_uri
        self.sa_secret_name = sa_secret_name
        self.client_secret_name = client_secret_name
        self.credentials = None
        self.token_refresh_interval = token_refresh_interval
        self._last_refresh_ts = 0

    # ---- Authentication ----
    def authenticate(self):
        """Authenticate to MLflow via service account or default credentials."""
        try:
            self.credentials = self._get_service_account_credentials()
            logger.info("Authenticated with service account for MLflow")
        except Exception as e:
            logger.warning(f"Service account authentication failed: {e}")
            self.credentials = self._get_default_credentials()
            logger.info("Authenticated with default GCP credentials for MLflow")

        self._apply_token()

    # ---- Token Refresh ----
    def refresh_token(self):
        """Refresh token if enough time has passed."""
        if not self.credentials:
            logger.warning("No credentials to refresh.")
            return

        now = time.time()
        if now - self._last_refresh_ts < self.token_refresh_interval:
            return

        try:
            self.credentials.refresh(Request())
            self._apply_token()
            self._last_refresh_ts = now
        except Exception as e:
            logger.error(f"Failed to refresh MLflow token: {e}")

    def _apply_token(self):
        os.environ["MLFLOW_TRACKING_TOKEN"] = self.credentials.token
        mlflow.set_tracking_uri(self.mlflow_uri)

    def _get_service_account_credentials(self):
        sa_info = json.loads(get_secret(self.sa_secret_name))
        audience = get_secret(self.client_secret_name)
        creds = service_account.IDTokenCredentials.from_service_account_info(
            sa_info, target_audience=audience
        )
        creds.refresh(Request())
        return creds

    def _get_default_credentials(self):
        creds = get_credentials()
        audience = get_secret(self.client_secret_name)
        id_creds = service_account.IDTokenCredentials(creds, target_audience=audience)
        id_creds.refresh(Request())
        return id_creds


def mlflow_refresh_token_decorator(auth_manager, func):
    """Wrap MLflow function to refresh token automatically before calling."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        auth_manager.refresh_token()
        return func(*args, **kwargs)

    return wrapper


@contextmanager
def mlflow_token_refresher_context(auth_manager):
    """
    Temporarily patch MLflow logging functions to refresh token before every call.
    """

    # Include all log_* and key setup methods
    patch_methods = _get_mlflow_log_functions_to_patch(
        extra_functions={"start_run", "end_run", "set_experiment"}
    )

    # Patch functions with token refresher
    originals = {}
    for method_name in patch_methods:
        func = getattr(mlflow, method_name, None)
        if callable(func):
            originals[method_name] = func
            setattr(
                mlflow, method_name, mlflow_refresh_token_decorator(auth_manager, func)
            )
    # Execute context block
    try:
        yield
    finally:
        # Restore original functions
        for method_name, original_func in originals.items():
            setattr(mlflow, method_name, original_func)


@conditional_mlflow()
def get_mlflow_experiment(experiment_name: str):
    """
    Get an MLflow experiment by name.
    Reactivate if it is deleted, or create if it doesn't exist.
    """
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)

    if experiment is None:
        # Experiment doesn't exist → create it
        logger.info(f"Creating new MLflow experiment: {experiment_name}")
        experiment_id = client.create_experiment(name=experiment_name)
        experiment = client.get_experiment(experiment_id)
    elif experiment.lifecycle_stage == "deleted":
        # Experiment is deleted → reactivate it
        logger.warning(
            f"MLflow experiment '{experiment_name}' is deleted. Reactivating it."
        )
        client.restore_experiment(experiment.experiment_id)
        experiment = client.get_experiment(experiment.experiment_id)

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
