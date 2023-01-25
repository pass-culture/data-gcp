import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
BASE_DIR = os.environ.get("BASE_DIR", "data-gcp/algo_training")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
MLFLOW_EHP_URI = "https://mlflow-ehp.internal-passculture.app/"
MLFLOW_PROD_URI = "https://mlflow.internal-passculture.app/"
TRAIN_DIR = os.environ.get("TRAIN_DIR", "/home/airflow/train")
MODEL_NAME = os.environ.get("MODEL_NAME", "")
SERVING_CONTAINER = "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-5:latest"
EXPERIMENT_NAME = os.environ.get(
    "EXPERIMENT_NAME", f"algo_training_v1.1_{ENV_SHORT_NAME}"
)

NUMBER_OF_PRESELECTED_OFFERS = 40
RECOMMENDATION_NUMBER = 10
SHUFFLE_RECOMMENDATION = True
EVALUATION_USER_NUMBER = 5000 if ENV_SHORT_NAME == "prod" else 200
EVALUATION_USER_NUMBER_DIVERSIFICATION = EVALUATION_USER_NUMBER // 2
