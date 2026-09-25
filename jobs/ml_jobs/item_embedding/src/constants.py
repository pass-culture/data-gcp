import os
import pathlib

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# MLflow tracking
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}@{GCP_PROJECT_ID}.iam.gserviceaccount.com"
MLFLOW_EXPERIMENT_NAME = f"item_embedding_v1.0_{ENV_SHORT_NAME}"
MLFLOW_RUN_ID_FILEPATH = (
    pathlib.Path(__file__).parent.parent / "mlflow_run_id.txt"
).as_posix()
MLFLOW_RUN_ID_COLUMN = "mlflow_run_id"
EMBEDDING_MODEL_COLUMN = "embedding_model"
EMBEDDING_DATE_COLUMN = "embedding_date"

# Hugging Face token secret name per environment
_HF_TOKEN_SECRET_NAMES: dict[str, str] = {
    "prod": "huggingface_token_prod",
    "stg": "huggingface_token_ehp",
    "dev": "huggingface_token_ehp",
}
HF_TOKEN_SECRET_NAME = _HF_TOKEN_SECRET_NAMES.get(
    ENV_SHORT_NAME, "huggingface_token_ehp"
)

# HF constants
BATCH_SIZE = 32

# Controls how many rows are read from GCS before a single
# embed_dataframe()/encode() call, independent of BigQuery's arbitrary,
# uneven per-file export sharding.
ROWS_PER_CHUNK = 50_000
# Cap the tokenized prompt length. Only ~2% of items exceed 512.
MAX_SEQ_LENGTH = 512
