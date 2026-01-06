import os

GCP_PROJECT = os.getenv("GCP_PROJECT", "passculture-data-ehp")
ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
ENVIRONMENT = "prod" if ENV_SHORT_NAME == "prod" else "ehp"
GCS_DATABASE_URI = (
    f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/search_db"
)
GCS_PARQUET_FILE = f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/chatbot_encoded_offers_metadata_{ENV_SHORT_NAME}"
TABLE_NAME = f"offers_{ENV_SHORT_NAME}"

SERVICE_ACCOUNT_EMAIL = (
    f"algo-training-{ENV_SHORT_NAME}@passculture-data-ehp.iam.gserviceaccount.com"
)
REGION = "europe-west1"
EXPERIMENT_NAME = f"search_edito_{ENV_SHORT_NAME}"
ENDPOINT_NAME = f"semantic-search-edito-endpoint-{ENV_SHORT_NAME}"
VERSION_NAME = "v1"
MODEL_TYPE = "custom"
INSTANCE_TYPE = "n1-standard-2"
TRAFFIC_PERCENTAGE = 100
MIN_NODES = 1
MAX_NODES = 10
SERVING_CONTAINER_BASE = "europe-west1-docker.pkg.dev/passculture-infra-prod"
SERVING_CONTAINER_REGISTRY = "pass-culture-artifact-registry/data-gcp/"
SERVING_CONTAINER_NAME = f"semantic_search_{ENV_SHORT_NAME}"
SERVING_CONTAINER = (
    f"{SERVING_CONTAINER_BASE}/{SERVING_CONTAINER_REGISTRY}/{SERVING_CONTAINER_NAME}"
)
