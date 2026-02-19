import os

ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
PROJECT_SUFFIX = "prod" if ENV_SHORT_NAME == "prod" else "ehp"
GCP_PROJECT_ID = f"passculture-data-{PROJECT_SUFFIX}"
SECRET_ID = f"allocine-data-{ENV_SHORT_NAME}-secret-token"
SECRET_VERSION = "latest"

# BigQuery
BQ_DATASET = f"raw_{ENV_SHORT_NAME}"
BQ_LOCATION = "europe-west1"
STAGING_TABLE = "staging_movies"
RAW_TABLE = "raw_movies"

# GCS
GCS_BUCKET = f"de-lake-{ENV_SHORT_NAME}"
POSTER_PREFIX = "posters"

# API
API_BASE_URL = "https://graph-api-proxy.allocine.fr/api"
API_MOVIE_ENDPOINT = "/query/movieList"
API_BATCH_SIZE = 100

# Rate limiting
RATE_LIMIT_CALLS = 100
RATE_LIMIT_PERIOD = 60  # seconds
RATE_LIMIT_BACKOFF = 10  # seconds
MAX_API_RETRIES = 3
