"""Configuration for Titelive ETL pipeline."""

import os
from enum import StrEnum

# Environment & GCP
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# BigQuery Tables
BIGQUERY_DATASET = f"raw_{ENV_SHORT_NAME}"
DEFAULT_SOURCE_TABLE = (
    f"{GCP_PROJECT_ID}.raw_{ENV_SHORT_NAME}.applicative_database_product"
)
DEFAULT_TARGET_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.raw_titelive_products"
PROVIDER_EVENT_TABLE = (
    f"{GCP_PROJECT_ID}.raw_{ENV_SHORT_NAME}.applicative_database_local_provider_event"
)
PRODUCT_TABLE = f"{GCP_PROJECT_ID}.raw_{ENV_SHORT_NAME}.applicative_database_product"
PRODUCT_MEDIATION_TABLE = (
    f"{GCP_PROJECT_ID}.raw_{ENV_SHORT_NAME}.applicative_database_product_mediation"
)


# Titelive Provider Configuration
TITELIVE_PROVIDER_ID = "1082"
TITELIVE_PROVIDER_IDS = [7, 8, 9, 10, 11, 15, 16, 17, 19, 20, 64, 1082, 1093, 2156]

# Titelive API
TITELIVE_BASE_URL = "https://catsearch.epagine.fr/v1"
TITELIVE_TOKEN_ENDPOINT = "https://login.epagine.fr/v1/login"

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
IMAGE_DOWNLOAD_GCS_PREFIX = "titelive/images"  # GCS path prefix for image storage
DE_DATALAKE_BUCKET_NAME = f"de-lake-{ENV_SHORT_NAME}"


# Product Categories
class TiteliveCategory(StrEnum):
    """Product categories for Titelive API."""

    PAPER = "paper"
    MUSIC = "music"


MUSIC_SUBCATEGORIES = {
    "SUPPORT_PHYSIQUE_MUSIQUE_VINYLE",
    "SUPPORT_PHYSIQUE_MUSIQUE_CD",
}
