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
    LOCAL_ENV,
)
from common.operators.kubernetes import KubernetesPodOperatorWithGitSync
from common.utils import get_airflow_schedule
from kubernetes.client import (
    V1Container,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1VolumeMount,
)

REPO_URL = "https://github.com/pass-culture/data-gcp"
DAG_PATH_IN_REPO = "orchestration/dags"
REPO_NAME = "data-gcp"

BRANCH = "k8s-social-network"
# IMAGE_TAG = "dev"
REGISTRY = (
    "europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry"
)
image_prefix = "data-gcp"

env_map = {
    "prod": "production",
    "stg": "staging",
    "dev": "development",
}

namespace = f"airflow-{env_map[ENV_SHORT_NAME]}"

DAG_NAME = "import_social_network_k8s_executor_v3"
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

MICROSERVICE_PATH_IN_REPO = "jobs/etl_jobs/external/instagram"


def make_pod_name(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")[:253]


def git_sync_override(branch: str) -> V1Pod:
    return V1Pod(
        spec=V1PodSpec(
            init_containers=[
                V1Container(
                    name="git-sync",
                    image="alpine/git",
                    command=["sh", "-c"],
                    args=[
                        f"git clone --depth 1 --branch {branch} {REPO_URL} /tmp/{REPO_NAME} && cp -r /tmp/{REPO_NAME}/{DAG_PATH_IN_REPO}/. /opt/airflow/dags/"
                    ],
                    volume_mounts=[
                        V1VolumeMount(
                            name="airflow-dags", mount_path="/opt/airflow/dags"
                        )
                    ],
                )
            ],
            containers=[
                V1Container(
                    name="base",
                    volume_mounts=[
                        V1VolumeMount(
                            name="airflow-dags",
                            mount_path="/opt/airflow/dags",
                        )
                    ],
                )
            ],
        )
    )


container_resources = V1ResourceRequirements(
    requests={
        "cpu": "0.2",
        "memory": "1Gi",
    },
    limits={
        "cpu": "0.5",
        "memory": "1Gi",
    },
)

kpo_common = dict(
    namespace=namespace,
    container_resources=container_resources,
    env_vars={
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "GIT_BRANCH": "{{ params.branch }}",
        "UV_CACHE_DIR": "/app/.cache/uv",
    },
    in_cluster=not LOCAL_ENV,
    get_logs=True,
    is_delete_operator_pod=False,
    on_finish_action="keep_pod",
    image_pull_policy="Always" if ENV_SHORT_NAME == "dev" else "IfNotPresent",
    queue="kubernetes",
    service_account_name="airflow-worker",
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
        "branch": Param(
            default="k8s-social-network",
            type="string",
        ),
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
):
    for social_network in ["instagram"]:
        KubernetesPodOperatorWithGitSync(
            task_id=f"{social_network}_etl",
            name=make_pod_name(social_network),
            # git-sync init container
            repo_url=REPO_URL,
            repo_name=REPO_NAME,
            microservice_path=MICROSERVICE_PATH_IN_REPO,
            branch="{{ params.branch }}",
            # main container
            image=f"{REGISTRY}/{image_prefix}/py310:{{{{ params.image_tag }}}}",
            cmds=["sh", "-c"],
            arguments=[
                "uv run main.py "
                "--start-date {% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_days) }} "
                "--end-date {% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_index) }}"
            ],
            executor_config={"pod_override": git_sync_override(branch=BRANCH)},
            **kpo_common,
        )
