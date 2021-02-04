import os

#### LEGACY ####
GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCP_REGION = "europe-west1"
EXTERNAL_CONNECTION_ID = "cloud_SQL_dump-prod-8-10-2020"
EXTERNAL_CONNECTION_ID_VM = "dump_scalingo_vm"
BASE32_JS_LIB_PATH = "gs://pass-culture-data/base32-encode/base32.js"

ENV_SHORT_NAME = "sbx"

#### NEW VARIABLES ####
GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "")

APPLICATIVE_EXTERNAL_CONNECTION_ID = os.environ.get(
    "APPLICATIVE_EXTERNAL_CONNECTION_ID", ""
)

DATA_GCS_BUCKET_NAME = os.environ.get(
    "DATA_GCS_BUCKET_NAME", f"data-bucket-{ENV_SHORT_NAME}"
)

BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"

APPLICATIVE_PREFIX = "applicative_database_"
MATOMO_PREFIX = "matomo_"
SURVEY_PREFIX = "survey_"