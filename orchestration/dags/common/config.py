import os
from enum import Enum

from airflow import configuration
from common.access_gcp_secrets import access_secret_data

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
ENVIRONMENT_NAME = {
    "dev": "development",
    "stg": "staging",
    "prod": "production",
}[ENV_SHORT_NAME]
DAG_FOLDER = os.environ.get("DAG_FOLDER", "dags/")
LOCAL_ENV = os.environ.get("LOCAL_ENV", None)
AIRFLOW_URI = {
    "dev": "airflow-dev.data.ehp.passculture.team",
    "stg": "airflow-stg.data.ehp.passculture.team",
    "prod": "airflow.data.passculture.team",
}[ENV_SHORT_NAME]

GCS_AIRFLOW_BUCKET = os.environ.get(
    "GCS_BUCKET", f"airflow-data-bucket-{ENV_SHORT_NAME}"
)

SSH_USER = os.environ.get("SSH_USER", "airflow")

GCP_REGION = "europe-west1"
GCE_ZONE = "europe-west1-b"

GCE_SA = os.environ.get("GCE_SA", f"algo-training-{ENV_SHORT_NAME}")

DEPLOYMENT_TAG = os.environ.get(
    "DEPLOYMENT_TAG", os.environ.get("LOCAL_TAG", "local-airflow")
)

GCE_BASE_PREFIX = f"{DEPLOYMENT_TAG}-{ENV_SHORT_NAME}"

GCE_TRAINING_INSTANCE = os.environ.get("GCE_TRAINING_INSTANCE", "algo-training-dev")
MLFLOW_BUCKET_NAME = os.environ.get("MLFLOW_BUCKET_NAME", "mlflow-bucket-ehp")
ML_BUCKET_TEMP = os.environ.get(
    "ML_BUCKET_TEMP", f"data-bucket-ml-temp-{ENV_SHORT_NAME}"
)
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
DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME = os.environ.get(
    "DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME", f"de-bigquery-data-archive-{ENV_SHORT_NAME}"
)
DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME = os.environ.get(
    "DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME", f"de-bigquery-data-import-{ENV_SHORT_NAME}"
)
DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME = os.environ.get(
    "DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME", f"de-bigquery-data-export-{ENV_SHORT_NAME}"
)
DS_DATA_ARCHIVE_BUCKET_NAME = os.environ.get(
    "DS_DATA_ARCHIVE_BUCKET_NAME", f"ds-data-archive-{ENV_SHORT_NAME}"
)
DE_TOOLS_BUCKET_NAME = os.environ.get(
    "DE_TOOLS_BUCKET_NAME", f"de-tools-{ENV_SHORT_NAME}"
)
DE_BIGQUERY_TMP_BACKUP_BUCKET_NAME = os.environ.get(
    "DE_BIGQUERY_TMP_BACKUP_BUCKET_NAME", f"bigquery-data-tmp-backup-{ENV_SHORT_NAME}"
)

BASE32_JS_LIB_PATH = f"gs://{DE_TOOLS_BUCKET_NAME}/base32-encode/base32.js"

if ENV_SHORT_NAME == "prod":
    ELEMENTARY_REPORT_URL = "https://dataquality.data.passculture.team/#/"
else:
    ELEMENTARY_REPORT_URL = None

# BQ Datasets
# TODO: Move this in a special file + construct as fstring and not env variables
BIGQUERY_SANDBOX_DATASET = os.environ.get(
    "BIGQUERY_SANDBOX_DATASET", f"sandbox_{ENV_SHORT_NAME}"
)
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET", f"raw_{ENV_SHORT_NAME}")
BIGQUERY_INT_RAW_DATASET = os.environ.get(
    "BIGQUERY_INT_RAW_DATASET", f"int_raw_{ENV_SHORT_NAME}"
)
BIGQUERY_SEED_DATASET = os.environ.get(
    "BIGQUERY_SEED_DATASET", f"seed_{ENV_SHORT_NAME}"
)
BIGQUERY_INT_API_GOUV_DATASET = os.environ.get(
    "BIGQUERY_INT_API_GOUV_DATASET", f"int_api_gouv_{ENV_SHORT_NAME}"
)
BIGQUERY_CLEAN_DATASET = os.environ.get(
    "BIGQUERY_CLEAN_DATASET", f"clean_{ENV_SHORT_NAME}"
)
BIGQUERY_ANALYTICS_DATASET = os.environ.get(
    "BIGQUERY_ANALYTICS_DATASET", f"analytics_{ENV_SHORT_NAME}"
)
BIGQUERY_INT_APPLICATIVE_DATASET = os.environ.get(
    "BIGQUERY_INT_APPLICATIVE_DATASET", f"int_applicative_{ENV_SHORT_NAME}"
)
BIGQUERY_INT_FIREBASE_DATASET = os.environ.get(
    "BIGQUERY_INT_FIREBASE_DATASET", f"int_firebase_{ENV_SHORT_NAME}"
)
BIGQUERY_INT_GEOLOCATION_DATASET = os.environ.get(
    "BIGQUERY_INT_GEOLOCATION_DATASET", f"int_geo_{ENV_SHORT_NAME}"
)
BIGQUERY_BACKEND_DATASET = os.environ.get(
    "BIGQUERY_BACKEND_DATASET", f"backend_{ENV_SHORT_NAME}"
)
BIGQUERY_ML_COMPLIANCE_DATASET = f"ml_compliance_{ENV_SHORT_NAME}"
BIGQUERY_ML_FEATURES_DATASET = f"ml_feat_{ENV_SHORT_NAME}"
BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET = f"ml_graph_recommendation_{ENV_SHORT_NAME}"
BIGQUERY_ML_LINKAGE_DATASET = f"ml_linkage_{ENV_SHORT_NAME}"
BIGQUERY_ML_OFFER_CATEGORIZATION_DATASET = f"ml_offer_categorization_{ENV_SHORT_NAME}"
BIGQUERY_ML_PREPROCESSING_DATASET = f"ml_preproc_{ENV_SHORT_NAME}"
BIGQUERY_ML_RECOMMENDATION_DATASET = f"ml_reco_{ENV_SHORT_NAME}"
BIGQUERY_ML_RETRIEVAL_DATASET = f"ml_retrieval_{ENV_SHORT_NAME}"
BIGQUERY_INT_RAW_DATASET = os.environ.get(
    "BIGQUERY_INT_RAW_DATASET", f"int_raw_{ENV_SHORT_NAME}"
)
BIGQUERY_TMP_DATASET = os.environ.get("BIGQUERY_TMP_DATASET", f"tmp_{ENV_SHORT_NAME}")

APPLICATIVE_PREFIX = "applicative_database_"

if ENV_SHORT_NAME == "prod":
    MEDIATION_URL = "passculture-metier-prod-production"
elif ENV_SHORT_NAME == "stg":
    MEDIATION_URL = "passculture-metier-ehp-staging"
else:
    MEDIATION_URL = "passculture-metier-ehp-testing"

if LOCAL_ENV is not None:
    PATH_TO_DBT_PROJECT = "/opt/airflow/dags/data_gcp_dbt"
else:
    PATH_TO_DBT_PROJECT = f"{DAG_FOLDER}/data_gcp_dbt".replace("//", "/")

if LOCAL_ENV is not None:
    PATH_TO_DBT_TARGET = "/opt/airflow/dags/data_gcp_dbt/target"
else:
    PATH_TO_DBT_TARGET = os.environ.get(
        "DBT_TARGET_PATH", "/opt/airflow/dags/data_gcp_dbt/target"
    )
EXCLUDED_TAGS = ["sandbox", "weekly", "monthly"]

if LOCAL_ENV is None:
    ELEMENTARY_PYTHON_PATH = (
        "/opt/python3.11/lib/python3.11/site-packages/elementary/monitor/dbt_project/"
    )
else:
    ELEMENTARY_PYTHON_PATH = os.environ.get("ELEMENTARY_PYTHON_PATH")

USE_INTERNAL_IP = False if LOCAL_ENV is not None else True

SLACK_TOKEN_DATA_QUALITY = access_secret_data(GCP_PROJECT_ID, "slack-token-elementary")
SLACK_CHANNEL_DATA_QUALITY = "alertes-data-quality"

CPU_INSTANCES_TYPES = {
    "standard": [
        "n1-standard-1",
        "n1-standard-2",
        "n1-standard-4",
        "n1-standard-8",
        "n1-standard-16",
        "n1-standard-32",
        "n1-standard-64",
        "n1-standard-96",
    ],
    "highmem": [
        "n1-highmem-2",
        "n1-highmem-4",
        "n1-highmem-8",
        "n1-highmem-16",
        "n1-highmem-32",
        "n1-highmem-64",
        "n1-highmem-96",
    ],
    "highcpu": [
        "n1-highcpu-2",
        "n1-highcpu-4",
        "n1-highcpu-8",
        "n1-highcpu-16",
        "n1-highcpu-32",
        "n1-highcpu-64",
        "n1-highcpu-96",
    ],
}
GPU_INSTANCES_TYPES = {
    "name": [
        "nvidia-tesla-t4",
        "nvidia-tesla-p4",
        "nvidia-tesla-p100",
        "nvidia-tesla-v100",
        "nvidia-tesla-a100",
    ],
    "count": [0, 1, 2, 4],
}

INSTANCES_TYPES = {"cpu": CPU_INSTANCES_TYPES, "gpu": GPU_INSTANCES_TYPES}

STOP_UPON_FAILURE_LABELS = ["long_ml"]
GCE_ZONES = [
    "europe-west1-b",
    "europe-west1-c",
    "europe-west1-d",
]  # Zones with GPUs and with lower CO2 emissions in europe-west1 (required to be in the proper VPC)


class DAG_TAGS(Enum):
    DS = "DS"
    DE = "DE"
    VM = "VM"
    DBT = "DBT"
    INCREMENTAL = "INCREMENTAL"
    POC = "POC"
    DRAGNDROP = "DRAG_&_DROP"


# UV Version
UV_VERSION = "0.5.2"

ENV_EMOJI = {
    "prod": ":volcano: *PROD* :volcano:",
    "stg": ":fire: *STAGING* :fire:",
    "dev": ":snowflake: *DEV* :snowflake:",
}


def get_env_emoji():
    base_url = configuration.get("webserver", "BASE_URL")
    base_emoji = ENV_EMOJI[ENV_SHORT_NAME]
    if LOCAL_ENV:
        return "**Local Environment**"
    if "localhost" in base_url:
        return f"{base_emoji} (k8s)"
    return base_emoji


def get_airflow_uri():
    base_url = configuration.get("webserver", "BASE_URL")
    if LOCAL_ENV:
        return base_url
    if "localhost" in base_url:
        return f"https://{AIRFLOW_URI}"
    return base_url
