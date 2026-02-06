import os

CONFIGS_PATH = "configs"

# Environment variables
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# MLflow
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"
MLFLOW_SECRET_NAME = "mlflow_client_id"
MLFLOW_RUN_ID_FILENAME = "mlflow_run_id"
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
