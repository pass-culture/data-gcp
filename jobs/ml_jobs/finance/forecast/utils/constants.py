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
    "https://mlflow.passculture.team/" if ENV_SHORT_NAME == "prod" else "https://mlflow.staging.passculture.team/"
)


## Plots
PRICING_LOWER_BOUND = 5e6
PRICING_UPPER_BOUND = 15e6

# Data freshness: warn when the most recent training/backtest day lags the
# execution date by more than this many days (e.g. source table not yet loaded).
DATA_FRESHNESS_WARNING_DAYS = 3
