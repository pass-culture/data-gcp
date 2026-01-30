"""Constants for the finance forecasting project.

This module contains configuration constants for GCP resources, BigQuery datasets,
MLflow tracking, and training parameters.
"""

import os

# GCP project and Environment
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-prod")

# BigQuery constants
if ENV_SHORT_NAME == "dev":  ## dev table only contains 2 rows, so get stg data instead
    FINANCE_DATASET = "ml_finance_stg"
else:
    FINANCE_DATASET = f"ml_finance_{ENV_SHORT_NAME}"

# MLflow Configuration
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"
MLFLOW_SECRET_NAME = "mlflow_client_id"
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
