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
from kubernetes.client import V1ResourceRequirements

REGISTRY = (
    "europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry"
)
image_prefix = "data-gcp/etl"

env_map = {
    "prod": "production",
    "stg": "staging",
    "dev": "development",
}

namespace = f"airflow-{env_map[ENV_SHORT_NAME]}"

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


container_resources = V1ResourceRequirements(
    requests={
        "cpu": "1",
        "memory": "1Gi",
    },
    limits={
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
        "image_tag": Param(
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
            namespace=namespace,
            image=f"{REGISTRY}/{image_prefix}/{social_network}:{{{{ params.image_tag }}}}",
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
                "GCP_PROJECT_ID": GCP_PROJECT_ID,
                "ENV_SHORT_NAME": ENV_SHORT_NAME,
            },
            labels={"dag": DAG_NAME, "task": f"run_{social_network}_etl"},
            service_account_name="airflow-worker",
            container_resources=container_resources,
        )
        # task.dry_run() Enable dry run for testing without executing the pod.
        task
