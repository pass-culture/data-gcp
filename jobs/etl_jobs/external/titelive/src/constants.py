"""Constants for Titelive API and ETL configuration."""

import os
from enum import StrEnum

# API Configuration
TITELIVE_TOKEN_ENDPOINT = "https://login.epagine.fr/v1/login"
TITELIVE_BASE_URL = "https://catsearch.epagine.fr/v1"

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "passculture-data-ehp")

# API Request Configuration
RESULTS_PER_PAGE = 120
BASE_TIMEOUT = 30
RESPONSE_ENCODING = "utf-8"
MAX_RESPONSES = 1_000_000
MAX_SEARCH_RESULTS = 20_000  # API limit for /search endpoint

# Batch Configuration
DEFAULT_BATCH_SIZE = 250  # For Mode 1: /ean batch processing (API limit)
FLUSH_THRESHOLD = 20_000  # Flush to BigQuery every N EANs (reduces BQ operations)
EAN_SEPARATOR = "|"  # Separator for multiple EANs in API request


class TiteliveCategory(StrEnum):
    """Product categories for Titelive API."""

    PAPER = "paper"
    MUSIC = "music"
