import os

from common.access_gcp_secrets import access_secret_data

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
DAG_FOLDER = os.environ.get("DAG_FOLDER", "dags/")
LOCAL_ENV = os.environ.get("LOCAL_ENV", None)

SSH_USER = os.environ.get("SSH_USER", "airflow")

GCP_REGION = "europe-west1"
GCE_ZONE = "europe-west1-b"
GCE_SUBNETWORK_ID = os.environ.get("GCE_SUBNETWORK_ID")
GCE_NETWORK_ID = os.environ.get("GCE_NETWORK_ID")
GCE_SA = os.environ.get("GCE_SA", f"algo-training-{ENV_SHORT_NAME}")
GCE_BASE_PREFIX = f"composer-{ENV_SHORT_NAME}"

BASE32_JS_LIB_PATH = f"gs://data-bucket-{ENV_SHORT_NAME}/base32-encode/base32.js"
GCE_TRAINING_INSTANCE = os.environ.get("GCE_TRAINING_INSTANCE", "algo-training-dev")
MLFLOW_BUCKET_NAME = os.environ.get("MLFLOW_BUCKET_NAME", "mlflow-bucket-ehp")
if ENV_SHORT_NAME != "prod":
    MLFLOW_URL = "https://mlflow.staging.passculture.team/"
else:
    MLFLOW_URL = "https://mlflow.passculture.team/"

APPLICATIVE_EXTERNAL_CONNECTION_ID = os.environ.get(
    "APPLICATIVE_EXTERNAL_CONNECTION_ID",
    "passculture-metier-ehp.europe-west1.metier-pcapi-testing-connection",
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

APPLICATIVE_PREFIX = "applicative_database_"

RECOMMENDATION_SQL_INSTANCE = os.environ.get(
    "RECOMMENDATION_SQL_INSTANCE", f"cloudsql-recommendation-{ENV_SHORT_NAME}-ew1"
)

CONNECTION_ID = os.environ.get("BIGQUERY_CONNECTION_RECOMMENDATION")

SLACK_CONN_ID = "slack_analytics"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT_ID, "slack-conn-password")

FAILED_STATES = ["failed", "upstream_failed", "skipped"]
ALLOWED_STATES = ["success"]

if ENV_SHORT_NAME == "prod":
    MEDIATION_URL = "passculture-metier-prod-production"
elif ENV_SHORT_NAME == "stg":
    MEDIATION_URL = "passculture-metier-ehp-staging"
else:
    MEDIATION_URL = "passculture-metier-ehp-testing"

PATH_TO_DBT_PROJECT = f"{DAG_FOLDER}/data_gcp_dbt".replace("//", "/")
PATH_TO_DBT_TARGET = os.environ.get("DBT_TARGET_PATH","/target")
