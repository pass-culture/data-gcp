import os

GCP_PROJECT = os.environ.get("GCP_PROJECT", "passculture-data-prod")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
MODEL_BASE_PATH = "model"
API_ENDPOINT = "europe-west1-aiplatform.googleapis.com"
LOCATION = "europe-west1"
