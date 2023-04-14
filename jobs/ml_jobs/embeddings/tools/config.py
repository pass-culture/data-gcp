import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
CONFIGS_PATH = os.environ.get("CONFIGS_PATH", "configs")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
MAX_OFFER_PER_BATCH = 50000
