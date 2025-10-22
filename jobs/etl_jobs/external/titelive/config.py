"""Configuration for Titelive ETL pipeline."""

import os
from enum import StrEnum

# Environment & GCP
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# BigQuery Tables
BIGQUERY_DATASET = "tmp_cdarnis_dev"
# BIGQUERY_DATASET = f"raw_{ENV_SHORT_NAME}"
# DEFAULT_SOURCE_TABLE = (f"{GCP_PROJECT_ID}.raw_{ENV_SHORT_NAME}.applicative_database_product" # noqa: E501
# DEFAULT_TARGET_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.raw_titelive_products"
# PROVIDER_EVENT_TABLE = f"{GCP_PROJECT_ID}.raw_{ENV_SHORT_NAME}.applicative_database_local_provider_event" # noqa: E501


DEFAULT_SOURCE_TABLE = "passculture-data-prod.raw_prod.applicative_database_product"
DEFAULT_TARGET_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.raw_titelive_products"
PROVIDER_EVENT_TABLE = (
    "passculture-data-prod.raw_prod." "applicative_database_local_provider_event"
)

# Titelive Provider Configuration
TITELIVE_PROVIDER_ID = "1082"

# Titelive API
TITELIVE_BASE_URL = "https://catsearch.epagine.fr/v1"
TITELIVE_TOKEN_ENDPOINT = "https://login.epagine.fr/v1/login"
TITELIVE_USERNAME_SECRET = "titelive_epagine_api_username"
TITELIVE_PASSWORD_SECRET = "titelive_epagine_api_password"

# API Configuration
RESULTS_PER_PAGE = 120
RESPONSE_ENCODING = "utf-8"
MAX_SEARCH_RESULTS = 20_000
EAN_SEPARATOR = "|"

# Batch Configuration
DEFAULT_BATCH_SIZE = 250  # API limit for /ean endpoint
MAIN_BATCH_SIZE = 20_000

# Image Download Configuration
IMAGE_DOWNLOAD_SUB_BATCH_SIZE = 1000  # Process images in chunks of 1000 EANs
IMAGE_DOWNLOAD_MAX_WORKERS = (os.cpu_count() - 1) * 5  # ThreadPoolExecutor max workers
IMAGE_DOWNLOAD_POOL_CONNECTIONS = 10  # HTTP adapter pool connections
IMAGE_DOWNLOAD_POOL_MAXSIZE = 20  # HTTP adapter pool max size
IMAGE_DOWNLOAD_TIMEOUT = 60  # HTTP request timeout in seconds
IMAGE_DOWNLOAD_GCS_PREFIX = "images/titelive"  # GCS path prefix for image storage
# DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME = f"de-bigquery-data-export-{ENV_SHORT_NAME}"
DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME = "data-team-sandbox-dev"


# Product Categories
class TiteliveCategory(StrEnum):
    """Product categories for Titelive API."""

    PAPER = "paper"
    MUSIC = "music"


MUSIC_SUBCATEGORIES = {
    "SUPPORT_PHYSIQUE_MUSIQUE_VINYLE",
    "SUPPORT_PHYSIQUE_MUSIQUE_CD",
}
