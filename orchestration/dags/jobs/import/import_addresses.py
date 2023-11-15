from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER
from common import macros
from common.config import (
    BIGQUERY_RAW_DATASET,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
)
from common.alerts import task_fail_slack_alert
from common.utils import getting_service_account_token, get_airflow_schedule
from common.operators.biquery import bigquery_job_task
from dependencies.addresses.import_addresses import (
    USER_LOCATIONS_SCHEMA,
    CLEAN_TABLES,
    ANALYTICS_TABLES,
)
ENV_SHORT_NAME ="dev"

GCE_INSTANCE = f"import-addresses-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/addresses"
dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

# FUNCTION_NAME = f"addresses_import_{ENV_SHORT_NAME}"
USER_LOCATIONS_TABLE = "user_locations"
schedule_interval = "0 * * * *" if ENV_SHORT_NAME == "prod" else "30 2 * * *"

default_args = {
    "start_date": datetime(2021, 3, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def branch_function(ti, **kwargs):
    xcom_value = ti.xcom_pull(task_ids="addresses_to_gcs", key="result")
    if "No new users !" not in xcom_value:
        return "import_addresses_to_bigquery"
    else:
        return "end"


with DAG(
    "dbt_crashtest",
    default_args=default_args,
    description="test",
    # every 10 minutes if prod once a day otherwise
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
) as dag:

    start = DummyOperator(task_id="start")
