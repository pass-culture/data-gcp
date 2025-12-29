import os

# GCP project and Environment
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-prod")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")

## BigQuery constants
FINANCE_DATASET = f"ml_finance_{ENV_SHORT_NAME}"
TABLE_WEEKLY_PRICING = "weekly_pricing"
TABLE_DAILY_PRICING = "daily_pricing"

## Training constants between all models
PREDICTION_FULL_HORIZON = "2026-12-31"  # Generate predictions up to this date
