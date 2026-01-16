"""Constants for the finance forecasting project.

This module contains configuration constants for GCP resources, BigQuery datasets,
MLflow tracking, and training parameters.
"""

import os

# GCP project and Environment
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-prod")

# BigQuery constants
FINANCE_DATASET = f"ml_finance_{ENV_SHORT_NAME}"

# Training constants
PREDICTION_FULL_HORIZON = "2026-12-31"  # Generate predictions up to this date

# MLflow Configuration
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"
MLFLOW_SECRET_NAME = "mlflow_client_id"
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
EXPERIMENT_NAME = f"finance_pricing_forecast_v0_{ENV_SHORT_NAME}"
