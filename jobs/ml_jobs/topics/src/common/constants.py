import os
import pathlib

# Infra
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# BigQuery
BIGQUERY_ML_FEATURES_DATASET = f"ml_feat_{ENV_SHORT_NAME}"
BIGQUERY_ML_INPUT_DATASET = f"ml_input_{ENV_SHORT_NAME}"
BIGQUERY_ML_SEMANTIC_EMBEDDING_DATASET = f"ml_semantic_embedding_{ENV_SHORT_NAME}"

# GCS
# ML_BUCKET (persistent model artifacts) has no existing convention elsewhere in
# the repo -- only ML_BUCKET_TEMP does (orchestration/dags/common/config.py).
# Confirm/create this bucket with infra before the reduction domain's first
# real model-saving run.
ML_BUCKET = os.environ.get("ML_BUCKET", f"data-bucket-ml-{ENV_SHORT_NAME}")
ML_BUCKET_TEMP = os.environ.get(
    "ML_BUCKET_TEMP", f"data-bucket-ml-temp-{ENV_SHORT_NAME}"
)

# MLflow tracking
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
# Reuses item_embedding's service account (already granted
# iam.serviceAccountTokenCreator on itself for JWT signing); confirm with infra
# whether `topics` should get its own SA before the first real deploy.
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}@{GCP_PROJECT_ID}.iam.gserviceaccount.com"
MLFLOW_EXPERIMENT_NAME = f"topics_v1.0_{ENV_SHORT_NAME}"
MLFLOW_RUN_ID_FILEPATH = (
    pathlib.Path(__file__).parent.parent.parent / "mlflow_run_id.txt"
).as_posix()
MLFLOW_RUN_ID_COLUMN = "mlflow_run_id"
