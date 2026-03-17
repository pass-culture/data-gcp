import os

ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "passculture-data-ehp")
SECRET_ID = f"allocine-data-{ENV_SHORT_NAME}-secret-token"
SECRET_VERSION = "latest"

# BigQuery
BQ_DATASET = f"raw_{ENV_SHORT_NAME}"
BQ_LOCATION = "europe-west1"
STAGING_TABLE = "tmp_allocine_movies"
RAW_TABLE = "allocine_movies"

# GCS
GCS_BUCKET = f"de-lake-{ENV_SHORT_NAME}"
POSTER_PREFIX = "allocine/movie/posters"

# API
API_BASE_URL = "https://graph-api-proxy.allocine.fr/api"
API_MOVIE_ENDPOINT = "/query/movieList"
API_REQUESTED_BATCH_SIZE = 50  # The API server caps responses at 50 regardless of this value

# Rate limiting and transient retry settings for API calls
RATE_LIMIT_CALLS = 100
RATE_LIMIT_PERIOD = 60  # seconds
RATE_LIMIT_BACKOFF = 10  # seconds
MAX_API_RETRIES = 3

# Retry missing posters later in time
POSTER_RETRY_DELAY = {
    'qty': 7,
    'unit': 'DAY'  # Must be a valid BigQuery time unit: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#date_and_time_types
}
