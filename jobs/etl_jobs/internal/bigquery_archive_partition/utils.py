import os

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
DATA_GCS_BUCKET_NAME = os.environ.get(
    "DATA_GCS_BUCKET_NAME", f"data-bucket-{ENV_SHORT_NAME}"
)

EXPORT_PATH = f"gs://{DATA_GCS_BUCKET_NAME}/historization"
