import os

CONFIGS_PATH = "configs"
# Environment variables
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
MODEL_DIR = os.environ.get("MODEL_DIR", "")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
TRAIN_DIR = os.environ.get("TRAIN_DIR", "/home/airflow/train")
MODEL_NAME = os.environ.get("MODEL_NAME", "")

# Infra Parameters
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
SERVING_CONTAINER = "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-5:latest"

# Recommendation Parameters
NUMBER_OF_PRESELECTED_OFFERS = 40
RECOMMENDATION_NUMBER = 10
SHUFFLE_RECOMMENDATION = True
EVALUATION_USER_NUMBER = 5000 if ENV_SHORT_NAME == "prod" else 200
EVALUATION_USER_NUMBER_DIVERSIFICATION = EVALUATION_USER_NUMBER // 2

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
