import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule

FUNCTION_NAME = f"siren_import_{ENV_SHORT_NAME}"
SIREN_FILENAME = "siren_data.csv"
schedule = "0 */6 * * *" if ENV_SHORT_NAME == "prod" else "30 */6 * * *"

DAG_NAME = "import_siren_v1"
GCE_INSTANCE = f"import-siren-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/siren"
MICROSERVICE_PATH = "jobs/etl_jobs/external/siren"
SIREN_ENV_VARS = {"PROJECT_NAME": GCP_PROJECT_ID}

default_dag_args = {
    "start_date": datetime.datetime(2021, 8, 25),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Siren from INSEE API",
    on_failure_callback=None,
    schedule=get_airflow_schedule(schedule),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    siren_to_bq = CustomKubernetesPodOperator(
        task_id="siren_to_bq",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=["main.py"],
        env_vars=SIREN_ENV_VARS,
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )

    start = EmptyOperator(task_id="start", dag=dag)

    end = EmptyOperator(task_id="end", dag=dag)

    start >> siren_to_bq >> end
