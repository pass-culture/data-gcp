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
GCE_VPC_SHARED_HOST_SUBNETWORK_ID = os.environ.get("GCE_VPC_SHARED_HOST_SUBNETWORK_ID")
GCE_VPC_SHARED_HOST_NETWORK_ID = os.environ.get("GCE_VPC_SHARED_HOST_NETWORK_ID")

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
BIGQUERY_SEED_DATASET = os.environ.get(
    "BIGQUERY_SEED_DATASET", f"seed_{ENV_SHORT_NAME}"
)
BIGQUERY_CLEAN_DATASET = os.environ.get(
    "BIGQUERY_CLEAN_DATASET", f"clean_{ENV_SHORT_NAME}"
)
BIGQUERY_ANALYTICS_DATASET = os.environ.get(
    "BIGQUERY_ANALYTICS_DATASET", f"analytics_{ENV_SHORT_NAME}"
)
BIGQUERY_INT_FIREBASE_DATASET = os.environ.get(
    "BIGQUERY_INT_FIREBASE_DATASET", f"int_firebase_{ENV_SHORT_NAME}"
)
BIGQUERY_INT_GEOLOCATION_DATASET = os.environ.get(
    "BIGQUERY_INT_GEOLOCATION_DATASET", f"int_geoloc_{ENV_SHORT_NAME}"
)
BIGQUERY_BACKEND_DATASET = os.environ.get(
    "BIGQUERY_BACKEND_DATASET", f"backend_{ENV_SHORT_NAME}"
)
BIGQUERY_ML_RECOMMENDATION_DATASET = os.environ.get(
    "BIGQUERY_ML_RECOMMENDATION_DATASET", f"ml_reco_{ENV_SHORT_NAME}"
)
BIGQUERY_ML_FEATURES_DATASET = os.environ.get(
    "BIGQUERY_ML_FEATURES_DATASET", f"ml_feat_{ENV_SHORT_NAME}"
)
BIGQUERY_ML_PREPROCESSING_DATASET = os.environ.get(
    "BIGQUERY_ML_PREPROCESSING_DATASET", f"ml_preproc_{ENV_SHORT_NAME}"
)
BIGQUERY_TMP_DATASET = os.environ.get("BIGQUERY_TMP_DATASET", f"tmp_{ENV_SHORT_NAME}")

APPLICATIVE_PREFIX = "applicative_database_"

RECOMMENDATION_SQL_INSTANCE = os.environ.get(
    "RECOMMENDATION_SQL_INSTANCE", f"cloudsql-recommendation-{ENV_SHORT_NAME}-ew1"
)

SLACK_CONN_ID = "slack_analytics"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT_ID, "slack-conn-password")

if ENV_SHORT_NAME == "prod":
    MEDIATION_URL = "passculture-metier-prod-production"
elif ENV_SHORT_NAME == "stg":
    MEDIATION_URL = "passculture-metier-ehp-staging"
else:
    MEDIATION_URL = "passculture-metier-ehp-testing"

if LOCAL_ENV != None:
    PATH_TO_DBT_PROJECT = "/opt/airflow/dags/data_gcp_dbt"
else:
    PATH_TO_DBT_PROJECT = f"{DAG_FOLDER}/data_gcp_dbt".replace("//", "/")

if LOCAL_ENV != None:
    PATH_TO_DBT_TARGET = "/opt/airflow/dags/data_gcp_dbt/target"
else:
    PATH_TO_DBT_TARGET = os.environ.get(
        "DBT_TARGET_PATH", "/opt/airflow/dags/data_gcp_dbt/target"
    )
EXCLUDED_TAGS = ["sandbox", "weekly", "monthly"]
