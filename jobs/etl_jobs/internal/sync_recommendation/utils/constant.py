import os
from pathlib import Path

SQL_PATH = Path(__file__).parent.parent / "sql"


ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
ENV_NAME = {
    "dev": "dev",
    "stg": "stg",
    "prod": "production",
}[ENV_SHORT_NAME]

PROJECT_NAME = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_ML_RECOMMENDATION_DATASET = f"ml_reco_{ENV_SHORT_NAME}"
BIGQUERY_ML_RETRIEVAL_DATASET = f"ml_retrieval_{ENV_SHORT_NAME}"
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_SEED_DATASET = f"seed_{ENV_SHORT_NAME}"
RECOMMENDATION_SQL_INSTANCE = f"cloudsql-recommendation-{ENV_NAME}-ew1"
REGION = "europe-west1"
# Constants for processing
MAX_RETRIES = 5

# Constants for database
DUCKDB_DATABASE_EXTENSIONS = ["postgres", "httpfs", "spatial"]
