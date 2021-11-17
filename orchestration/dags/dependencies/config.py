import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "project-test-ci")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "project-test-ci")
GCP_REGION = "europe-west1"
GCE_ZONE = "europe-west1-b"
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

BASE32_JS_LIB_PATH = f"gs://data-bucket-{ENV_SHORT_NAME}/base32-encode/base32.js"

APPLICATIVE_EXTERNAL_CONNECTION_ID = os.environ.get(
    "APPLICATIVE_EXTERNAL_CONNECTION_ID", ""
)
MATOMO_EXTERNAL_CONNECTION_ID = os.environ.get("MATOMO_EXTERNAL_CONNECTION_ID", "")
DATA_GCS_BUCKET_NAME = os.environ.get(
    "DATA_GCS_BUCKET_NAME", f"data-bucket-{ENV_SHORT_NAME}"
)

BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET", f"raw_{ENV_SHORT_NAME}")
BIGQUERY_CLEAN_DATASET = os.environ.get(
    "BIGQUERY_CLEAN_DATASET", f"clean_{ENV_SHORT_NAME}"
)
BIGQUERY_ANALYTICS_DATASET = os.environ.get(
    "BIGQUERY_ANALYTICS_DATASET", f"analytics_{ENV_SHORT_NAME}"
)

APPLICATIVE_PREFIX = "applicative_database_"
MATOMO_PREFIX = "matomo_"
SURVEY_PREFIX = "survey_"

TABLE_AB_TESTING = "abc_testing_20211029_v1v2"