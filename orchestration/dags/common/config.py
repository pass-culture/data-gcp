import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
DAG_FOLDER = os.environ.get("DAG_FOLDER", "dags/")

SSH_USER = os.environ.get("SSH_USER", "airflow")

GCP_REGION = "europe-west1"
GCE_ZONE = "europe-west1-b"
VM_SUBNET = os.environ.get("VM_SUBNET", f"data-{ENV_SHORT_NAME}-public")
VM_NETWORK = os.environ.get("VM_NETWORK", f"vpc-data-{ENV_SHORT_NAME}")
VM_SA = os.environ.get("VM_SA", f"algo-training-{ENV_SHORT_NAME}")


BASE32_JS_LIB_PATH = f"gs://data-bucket-{ENV_SHORT_NAME}/base32-encode/base32.js"
APPLICATIVE_EXTERNAL_CONNECTION_ID = os.environ.get(
    "APPLICATIVE_EXTERNAL_CONNECTION_ID", ""
)
METABASE_EXTERNAL_CONNECTION_ID = os.environ.get("METABASE_EXTERNAL_CONNECTION_ID", "")
DATA_GCS_BUCKET_NAME = os.environ.get(
    "DATA_GCS_BUCKET_NAME", f"data-bucket-{ENV_SHORT_NAME}"
)

BIGQUERY_SANDBOX_DATASET = os.environ.get(
    "BIGQUERY_SANDBOX_DATASET", f"sandbox_{ENV_SHORT_NAME}"
)
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET", f"raw_{ENV_SHORT_NAME}")
BIGQUERY_CLEAN_DATASET = os.environ.get(
    "BIGQUERY_CLEAN_DATASET", f"clean_{ENV_SHORT_NAME}"
)
BIGQUERY_ANALYTICS_DATASET = os.environ.get(
    "BIGQUERY_ANALYTICS_DATASET", f"analytics_{ENV_SHORT_NAME}"
)
BIGQUERY_BACKEND_DATASET = os.environ.get(
    "BIGQUERY_BACKEND_DATASET", f"backend_{ENV_SHORT_NAME}"
)
BIGQUERY_TMP_DATASET = os.environ.get("BIGQUERY_TMP_DATASET", f"tmp_{ENV_SHORT_NAME}")

BIGQUERY_OPEN_DATA_PROJECT = (
    "passculture-opendata-prod"
    if GCP_PROJECT_ID == "passculture-data-prod"
    else "passculture-opendata-ehp"
)
BIGQUERY_OPEN_DATA_PUBLIC_DATASET = os.environ.get(
    "BIGQUERY_OPEN_DATA_PUBLIC_DATASET", f"public_{ENV_SHORT_NAME}"
)

APPLICATIVE_PREFIX = "applicative_database_"
QPI_TABLE = "qpi_answers_v4"
RECOMMENDATION_SQL_INSTANCE = os.environ.get(
    "RECOMMENDATION_SQL_INSTANCE", f"cloudsql-recommendation-{ENV_SHORT_NAME}"
)
RECOMMENDATION_SQL_BASE = os.environ.get(
    "RECOMMENDATION_SQL_BASE", f"cloudsql-recommendation-{ENV_SHORT_NAME}"
)

CONNECTION_ID = (
    os.environ.get("BIGQUERY_CONNECTION_RECOMMENDATION")
    if ENV_SHORT_NAME != "prod"
    else f"{GCP_PROJECT_ID}.{GCP_REGION}.cloudsql-recommendation-production-bq-connection"
)
