"""Constants for the finance forecasting project.

This module contains configuration constants for GCP resources, BigQuery datasets,
MLflow tracking, and training parameters.
"""

import os

# GCP project and Environment
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")

# MLflow Configuration
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}@{GCP_PROJECT_ID}.iam.gserviceaccount.com"
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
