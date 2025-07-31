import os

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
MODEL_BASE_PATH = "model"
