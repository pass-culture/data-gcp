from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common.alerts.task_fail import task_fail_slack_alert
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule

MICROSERVICE_PATH = "jobs/etl_jobs/internal/import_api_referentials"
DAG_NAME = "import_api_referentials"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Continuous update of api model to BQ",
    schedule=get_airflow_schedule("30 0 * * 1"),  # import every monday at 00:00
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    _kpo_common = {
        "orchestration_mode": "celery",
        "queue": "k8s-watcher",
        "runtime_mode": "gitsynced",
        "runtime_branch": "{{ params.branch }}",
        "runtime_image": "py313",
        "runtime_image_tag": "v1",
        "microservice_path": MICROSERVICE_PATH,
        "env_vars": {
            "GCP_PROJECT": GCP_PROJECT_ID,
            "CORS_ALLOWED_ORIGINS": "",
            "CORS_ALLOWED_ORIGINS_NATIVE": "",
            "CORS_ALLOWED_ORIGINS_AUTH": "",
            "CORS_ALLOWED_ORIGINS_ADAGE_IFRAME": "",
        },
        "container_resources": DEFAULT_CONTAINER_RESOURCES,
    }

    start = EmptyOperator(task_id="start")

    subcategories_op = CustomKubernetesPodOperator(
        task_id="import_subcategories",
        arguments=[
            "main.py",
            "--job_type",
            "subcategories",
            "--gcp_project_id",
            GCP_PROJECT_ID,
            "--env_short_name",
            ENV_SHORT_NAME,
        ],
        **_kpo_common,
    )

    types_op = CustomKubernetesPodOperator(
        task_id="import_types",
        arguments=[
            "main.py",
            "--job_type",
            "types",
            "--gcp_project_id",
            GCP_PROJECT_ID,
            "--env_short_name",
            ENV_SHORT_NAME,
        ],
        **_kpo_common,
    )

    end = EmptyOperator(task_id="end")

    start >> subcategories_op >> types_op >> end
