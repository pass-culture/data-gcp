import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

MLFLOW_EHP_URI = "https://mlflow.staging.passculture.team/"
MLFLOW_PROD_URI = "https://mlflow.passculture.team/"
MLFLOW_SECRET_NAME = "mlflow_client_id"

EXPERIMENT_BASE_NAME = "artist_linkage"
