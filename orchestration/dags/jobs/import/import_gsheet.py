import datetime

from airflow import DAG
from airflow.models import Param
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

DAG_NAME = "import_gsheet"
MICROSERVICE_PATH = "jobs/etl_jobs/external/gsheet"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Adhoc Gsheet",
    on_failure_callback=None,
    schedule=get_airflow_schedule("0 1 * * *"),
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
    gsheet_to_bq = CustomKubernetesPodOperator(
        task_id="gsheet_to_bq",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=["main.py"],
        env_vars={"PROJECT_NAME": GCP_PROJECT_ID},
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )
