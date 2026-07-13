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
from common.operators.bigquery import bigquery_job_task
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule
from dependencies.downloads.import_downloads import ANALYTICS_TABLES

MICROSERVICE_PATH = "jobs/etl_jobs/external/downloads"
DAG_NAME = "import_downloads"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import downloads tables",
    schedule=get_airflow_schedule("00 02 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "execution_date": Param(
            default="",
            type="string",
            description="Execution date in YYYY-MM-DD format. If not provided, it will default to ds.",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    import_google_downloads_data_to_bigquery = CustomKubernetesPodOperator(
        task_id="import_google_downloads_data_to_bigquery",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=[
            "main.py",
            "--provider",
            "google",
            "--execution-date",
            "{{ params.execution_date or ds }}",
        ],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
        env_vars={"PROJECT_NAME": GCP_PROJECT_ID},
    )

    import_apple_downloads_data_to_bigquery = CustomKubernetesPodOperator(
        task_id="import_apple_downloads_data_to_bigquery",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=[
            "main.py",
            "--provider",
            "apple",
            "--execution-date",
            "{{ params.execution_date or ds }}",
        ],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
        env_vars={"PROJECT_NAME": GCP_PROJECT_ID},
    )

    # only downloads included here
    analytics_tasks = []
    for table, params in ANALYTICS_TABLES.items():
        task = bigquery_job_task(table=table, dag=dag, job_params=params)
        analytics_tasks.append(task)

    end = EmptyOperator(task_id="end", dag=dag)

    for analytics_task in analytics_tasks:
        (
            [
                import_google_downloads_data_to_bigquery,
                import_apple_downloads_data_to_bigquery,
            ]
            >> analytics_task
            >> end
        )
