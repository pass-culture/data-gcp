import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.kubernetes import EasyKubernetesPodOperator
from common.utils import get_airflow_schedule
from kubernetes.client import V1ResourceRequirements

DAG_NAME = "import_social_network_k8s"
default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}

schedule_dict = {
    "prod": "0 2 * * *",
    "stg": "0 3 * * *",
    "dev": None,
}[ENV_SHORT_NAME]


container_resources = V1ResourceRequirements(
    requests={
        "cpu": "0.2",
        "memory": "500Mi",
    },
    limits={
        "cpu": "0.5",
        "memory": "1Gi",
    },
)

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Social Network Data",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule(schedule_dict),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=240),
    params={
        "runtime_image_tag": Param(
            default="dev",
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
        task = EasyKubernetesPodOperator(
            task_id=f"{social_network}_etl",
            orchestration_mode="celery",  # use a celery worker to request the task to k8s and free it up with deferrable=True
            runtime_mode="containerized",
            runtime_image=f"etl/{social_network}",
            runtime_image_tag="{{ params.runtime_image_tag }}",
            arguments=[
                "main.py",
                "--start-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_days) }}",
                "--end-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_index) }}",
            ],
            container_resources=container_resources,
            deferrable=True,
            poll_interval=180,
        )

        task
