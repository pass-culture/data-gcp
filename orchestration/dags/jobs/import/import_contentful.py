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

MICROSERVICE_PATH = "jobs/etl_jobs/external/contentful"
DAG_NAME = "import_contentful"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 2,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import contentful tables",
    on_failure_callback=None,
    schedule=get_airflow_schedule("30 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "playlists_names": Param(
            default="",
            type="string",
            description="Comma separated list of playlists names to import: p1,p2,p3",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    import_contentful_data_to_bigquery = CustomKubernetesPodOperator(
        task_id="import_contentful_data_to_bigquery",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=[
            "main.py",
            "{% if params.playlists_names and params.playlists_names | trim != '' %}--playlists-names{% endif %}",
            "{% if params.playlists_names and params.playlists_names | trim != '' %}{{ params.playlists_names }}{% endif %}",
        ],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )

    import_contentful_data_to_bigquery
