import os

CONFIGS_PATH = "configs"

# Environment variables
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
MODEL_DIR = os.environ.get("MODEL_DIR", "")
TRAIN_DIR = os.environ.get("TRAIN_DIR", "/home/airflow/train")
MODEL_NAME = os.environ.get("MODEL_NAME", "")
GCP_PROJECT_ID = (
    "passculture-data-prod" if ENV_SHORT_NAME == "prod" else "passculture-data-ehp"
)

# Infra Parameters
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"

# Recommendation Parameters
EVALUATION_USER_NUMBER = 50_000 if ENV_SHORT_NAME == "prod" else 5000
LIST_K = [50, 100, 250, 500, 1000]
USER_BATCH_SIZE = 100
ALL_USERS = False

# MLflow
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"
MLFLOW_SECRET_NAME = "mlflow_client_id"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
MLFLOW_RUN_ID_FILENAME = "mlflow_run_uuid"
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)

# Embeddings storage variables
EMBEDDING_EXPIRATION_DELAY_MS = 1000 * 60 * 60 * 24 * 30 * 6  # 6 Months
