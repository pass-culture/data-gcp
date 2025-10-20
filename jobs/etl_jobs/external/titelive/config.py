"""Configuration for Titelive ETL pipeline."""

import os

# Environment Configuration
PROJECT_NAME = os.environ.get("PROJECT_NAME", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# GCP Configuration
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "tmp_cdarnis_dev")

# Source Table Configuration (variabilized by environment)
DEFAULT_SOURCE_TABLE = (
    f"{PROJECT_NAME}.raw_{ENV_SHORT_NAME}.applicative_database_product"
)

# Default Table Names (can be overridden via CLI)
DEFAULT_TARGET_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__products"
DEFAULT_TRACKING_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__tracking"
DEFAULT_PROCESSED_EANS_TABLE = (
    f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__processed_eans"
)
DEFAULT_TEMP_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__gcs_raw"

DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME = f"de-bigquery-data-export-{ENV_SHORT_NAME}"

# API Configuration
TITELIVE_API_BASE_URL = "https://catsearch.epagine.fr/v1"
TITELIVE_TOKEN_ENDPOINT = "https://login.epagine.fr/v1/login"

# Secret Manager Configuration
TITELIVE_USERNAME_SECRET = "titelive_epagine_api_username"
TITELIVE_PASSWORD_SECRET = "titelive_epagine_api_password"

# Processing Configuration
DEFAULT_BATCH_SIZE = 50  # For Mode 1: batch processing
RESULTS_PER_PAGE = 120  # For Mode 3: pagination
MAX_SEARCH_RESULTS = 20_000  # API limit

# Rate Limiting
RATE_LIMIT_CALLS = 10  # Number of API calls allowed
RATE_LIMIT_PERIOD = 1  # Time period in seconds
