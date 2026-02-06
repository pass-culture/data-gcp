import os
from multiprocessing import cpu_count

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
CONFIGS_PATH = os.environ.get("CONFIGS_PATH", "configs")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
BIGQUERY_TMP_DATASET = f"tmp_{ENV_SHORT_NAME}"

PRIMARY_KEY = "item_id"
NULLIFY_DEPRECATED_COLUMNS = True
FINGERPRINT_COLUMN = "content_hash"

TRANSFORMER_BATCH_SIZE = 128
IMAGE_DIR = "./img/"

MAX_PROCESS = max(1, cpu_count())
