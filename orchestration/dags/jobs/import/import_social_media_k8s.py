import datetime
import re

from airflow import DAG
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from common import macros

# from kubernetes.client import V1Toleration
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.utils import get_airflow_schedule

REGISTRY_REGION = "europe-west1-docker.pkg.dev"
REGISTRY_PROJECT = "passculture-infra-prod"


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


def make_pod_name(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")[:253]


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
        "tag": Param(
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
):
    for social_network in ["instagram"]:
        task = KubernetesPodOperator(
            task_id=f"run_{social_network}_etl",
            name=make_pod_name(f"{social_network}"),
            namespace="airflow",
            image=f"{REGISTRY_REGION}-docker.pkg.dev/{REGISTRY_PROJECT}/data-gcp/etl/{social_network}:{{{{ params.tag }}}}",
            arguments=[
                "--start-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_days) }}",
                "--end-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_index) }}",
            ],
            get_logs=True,
            is_delete_operator_pod=False,
            in_cluster=True,
            env_vars={
                "GCP_PROJECT_ID": "passculture-data-ehp",
                "ENV_SHORT_NAME": "dev",
            },
            # executor_config={
            #     "KubernetesExecutor": {"namespace": "airflow"}
            # },
            # queue="kubernetes",
            # node_selector={"role": "airflow-kube-worker"},
            # tolerations=[
            #     V1Toleration(
            #         key="role",
            #         operator="Equal",
            #         value="airflow-kube-worker",
            #         effect="NoSchedule",
            #     )
            # ], # Ensure the pod can be scheduled on the dedicated node.
        )

        task  # Enable dry run for testing without executing the pod.
