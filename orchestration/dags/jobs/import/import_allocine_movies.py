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
)
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule

MICROSERVICE_PATH = "jobs/etl_jobs/external/allocine"
DAG_NAME = "import_allocine_movies"

default_args = {
    "start_date": datetime.datetime(2026, 1, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=2),
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Import allocine movies data",
    schedule=get_airflow_schedule("0 0 * * *"),  # import every day at 00:00
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "poster_retries": Param(
            default=3,
            type="integer",
            description="Max number of dag run to attempt poster download if url is missing.",
        ),
        "poster_download_backoff_unit": Param(
            default="DAY",
            enum=["DAY", "WEEK", "MONTH", "YEAR"],
            description="Time unit between poster download retries.",
        ),
        "poster_download_backoff": Param(
            default=7,
            type="integer",
            description="Quantity of time units to wait between download retries.",
        ),
    },
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    start = EmptyOperator(task_id="start")

    synchronize_movies = CustomKubernetesPodOperator(
        task_id="synchronize_movies",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=["main.py", "sync-movies"],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )

    synchronize_posters = CustomKubernetesPodOperator(
        task_id="synchronize_posters",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=[
            "main.py",
            "sync-posters",
            "--max-retries",
            "{{ params.poster_retries }}",
            "--poster-download-backoff",
            "{{ params.poster_download_backoff }}",
            "--poster-download-backoff-unit",
            "{{ params.poster_download_backoff_unit }}",
        ],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> synchronize_movies >> synchronize_posters >> end
