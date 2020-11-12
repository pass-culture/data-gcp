GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCP_REGION = "europe-west1"
CLOUDSQL_DATABASE = "cloud_SQL_dump-prod-8-10-2020"
BASE32_JS_LIB_PATH = "gs://pass-culture-data/base32-encode/base32.js"

BIGQUERY_AIRFLOW_DATASET = 'pcdata_poc_bqds_airflow'
DATA_ANALYTICS_TABLES = [
    "user", "provider", "offerer", "bank_information", "booking", "payment", "venue", "user_offerer", "offer", "stock",
    "favorite", "venue_type", "venue_label"
]
