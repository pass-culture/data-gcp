import os

# core.utils reads these at import time — set them before any application import.
os.environ.setdefault("ENV_SHORT_NAME", "dev")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
