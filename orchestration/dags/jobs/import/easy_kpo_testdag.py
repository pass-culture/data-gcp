import datetime
import re

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
from kubernetes.client import V1ResourceRequirements

REGISTRY = (
    "europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry"
)
MICROSERVICE_PATH = "jobs/etl_jobs/external/instagram"

DAG_NAME = "easy_kpo_testdag"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "project_id": GCP_PROJECT_ID,
}

container_resources = V1ResourceRequirements(
    requests={"cpu": "0.2", "memory": "1Gi"},
    limits={"cpu": "0.5", "memory": "1Gi"},
)

kpo_common = dict(
    container_resources=container_resources,
    env_vars={
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
    },
)

# For gitsynced: single shell string — operator wraps in `sh -c`.
_ARGS_SHELL = (
    "main.py "
    "--start-date "
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}"
    "{{ add_days(base, params.n_days) }} "
    "--end-date "
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}"
    "{{ add_days(base, params.n_index) }}"
)

# For containerized: split list — each item becomes a separate arg to the image ENTRYPOINT.
_ARGS_LIST = [
    "main.py",
    "--start-date",
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_days) }}",
    "--end-date",
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_index) }}",
]


def make_pod_name(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")[:253]


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="EasyKubernetesPodOperator — all four mode combinations",
    schedule_interval=None,
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=60),
    params={
        "branch": Param(
            default="k8s-social-network",
            type="string",
            description="Git branch for the job pod (gitsynced runtime)",
        ),
        "image_tag": Param(
            default="dev",
            type="string",
            description="Image tag for containerized runtime tasks",
        ),
        "n_days": Param(
            default=-7,
            type="integer",
            description="Days before execution date for start date",
        ),
        "n_index": Param(
            default=0,
            type="integer",
            description="Offset from execution date for end date",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
):
    # ── celery + gitsynced ──────────────────────────────────────────────────
    # Operator forces `sh -c` and prepends `cd /app && uv run `.
    # Pass the script name + args as a single shell string.
    EasyKubernetesPodOperator(
        task_id="celery_gitsynced",
        name=make_pod_name("celery-gitsynced"),
        runtime_mode="gitsynced",
        branch="{{ params.branch }}",
        microservice_path=MICROSERVICE_PATH,
        arguments=[_ARGS_SHELL],
        **kpo_common,
    )

    # ── celery + containerized ──────────────────────────────────────────────
    # Image ENTRYPOINT is preserved. Pass each flag/value as a separate list
    # item so Kubernetes delivers them as individual args to the entrypoint.
    EasyKubernetesPodOperator(
        task_id="celery_containerized",
        name=make_pod_name("celery-containerized"),
        runtime_mode="containerized",
        image=f"{REGISTRY}/data-gcp/etl/instagram:{{{{ params.image_tag }}}}",
        arguments=_ARGS_LIST,
        **kpo_common,
    )

    # ── kubernetes + gitsynced ──────────────────────────────────────────────
    # Operator forces `sh -c` and prepends `cd /app && uv run `.
    # Pass the script name + args as a single shell string.
    EasyKubernetesPodOperator(
        task_id="k8s_gitsynced",
        name=make_pod_name("k8s-gitsynced"),
        orchestration_mode="kubernetes",
        runtime_mode="gitsynced",
        branch="{{ params.branch }}",
        microservice_path=MICROSERVICE_PATH,
        arguments=[_ARGS_SHELL],
        # deferrable=True,
        # poll_interval=60,
        **kpo_common,
    )

    # ── kubernetes + containerized ──────────────────────────────────────────
    # Image ENTRYPOINT is preserved. Pass each flag/value as a separate list
    # item so Kubernetes delivers them as individual args to the entrypoint.
    EasyKubernetesPodOperator(
        task_id="k8s_containerized",
        name=make_pod_name("k8s-containerized"),
        orchestration_mode="kubernetes",
        runtime_mode="containerized",
        image=f"{REGISTRY}/data-gcp/etl/instagram:{{{{ params.image_tag }}}}",
        arguments=_ARGS_LIST,
        # deferrable=True,
        # poll_interval=60,
        **kpo_common,
    )
