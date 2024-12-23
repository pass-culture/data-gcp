import os
from multiprocessing import cpu_count

from utils.access_gcp_secrets import access_secret

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
API_TOKEN_SECRET_ID = os.environ.get("API_TOKEN_SECRET_ID")
API_URL_SECRET_ID = os.environ.get("API_URL_SECRET_ID")

try:
    API_TOKEN = access_secret(GCP_PROJECT_ID, API_TOKEN_SECRET_ID)
except Exception:
    API_TOKEN = "test_token"
# TODO: Add secrets via infra
try:
    API_URL = access_secret(GCP_PROJECT_ID, API_URL_SECRET_ID)
except Exception:
    API_URL = "test_url"

APP_CONFIG = {
    "URL": API_URL,
    "TOKEN": API_TOKEN,
    "route": "similar_offers",
}
N_RECO_DISPLAY = 10
MAX_PROCESS = 2 if ENV_SHORT_NAME == "dev" else cpu_count() - 2
