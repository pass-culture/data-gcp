GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCP_REGION = "europe-west1"
EXTERNAL_CONNECTION_ID = "cloud_SQL_dump-prod-8-10-2020"
EXTERNAL_CONNECTION_ID_VM = "dump_scalingo_vm"
BASE32_JS_LIB_PATH = "gs://pass-culture-data/base32-encode/base32.js"

ENV_SHORT_NAME = "sbx"

BIGQUERY_RAW_DATASET = f"raw-{ENV_SHORT_NAME}"
BIGQUERY_CLEAN_DATASET = f"clean-{ENV_SHORT_NAME}"
BIGQUERY_ANALYTICS_DATASET = f"analytics-{ENV_SHORT_NAME}"
