import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import DAG_FOLDER, DAG_TAGS, ENV_SHORT_NAME, GCP_PROJECT_ID
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "import_harvestr"

default_dag_args = {
    "start_date": datetime.datetime(2020, 1, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Harvestr Data",
    on_failure_callback=None,
    schedule=get_airflow_schedule(SCHEDULE_DICT["import_harvestr"][ENV_SHORT_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=240),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "start_date": Param(
            default="",
            type="string",
        ),
        "end_date": Param(
            default="",
            type="string",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
):
    task = CustomKubernetesPodOperator(
        task_id="harvestr_etl",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path="jobs/etl_jobs/external/harvestr",
        arguments=[
            "main.py",
            "--start-date",
            "{{ params.start_date or (ds | add_days(-1)) }}",
            "--end-date",
            "{{ params.end_date or ds }}",
        ],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )
