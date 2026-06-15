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

DAG_NAME = "import_social_network"
default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}

schedule_dict = {
    "prod": "0 2 * * *",
    "stg": "0 3 * * *",
    "dev": None,
}[ENV_SHORT_NAME]

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Social Network Data",
    on_failure_callback=None,
    schedule=get_airflow_schedule(schedule_dict),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=240),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "n_days": Param(
            default=-7,
            type="integer",
            description="Number of days to go back from the execution date for the start date (e.g., -1 for yesterday).",
        ),
        "n_index": Param(
            default=0,
            type="integer",
            description="Offset in days from the execution date for the end date (e.g., 0 for the execution date, -1 for yesterday).",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
):
    for social_network in ["instagram", "tiktok"]:
        task = CustomKubernetesPodOperator(
            task_id=f"{social_network}_etl",
            orchestration_mode="celery",  # use a celery worker to request the task to k8s
            queue="k8s-watcher",  # we route the task to a specific queue that is listened by a celery-worker pool with higher concurency than the default one, this allow running multiple k8s tasks in parallel, while freeing up the default celery-worker for other non-k8s tasks
            runtime_mode="gitsynced",
            runtime_branch="{{ params.branch }}",
            runtime_image="py313",
            runtime_image_tag="v1",
            microservice_path=f"jobs/etl_jobs/external/{social_network}",
            arguments=[
                "main.py",
                "--start-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_days) }}",
                "--end-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_index) }}",
            ],
            container_resources=DEFAULT_CONTAINER_RESOURCES,
        )

        task
