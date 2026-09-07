from datetime import timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

# Airflow params
DAG_NAME = "build_and_push_semantic_retrieval_api"
default_args = {
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# GCS Paths / Filenames
BASE_DIR = "data-gcp/jobs/ml_jobs/retrieval_vector/"

# GCE
INSTANCE_NAME = f"build-and-push-semantic-retrieval-api-{ENV_SHORT_NAME}"
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-2",
    "prod": "n1-standard-2",
}[ENV_SHORT_NAME]
DEFAULT_CONTAINER_WORKER = "1"

# Registry / MLFlow (shares the retrieval-vector image namespace; own experiment)
ARTIFACT_REGISTRY_BASE_PATH = f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/{ENV_SHORT_NAME}"
SEMANTIC_MODEL_NAME = "embeddinggemma"
SEMANTIC_MODEL_VERSION = f"semantic_item_retrieval_v1.0_{ENV_SHORT_NAME}"

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Build & push the semantic item retrieval API container ",
    schedule=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=INSTANCE_TYPE,
            type="string",
        ),
        "instance_name": Param(
            default=INSTANCE_NAME,
            type="string",
        ),
        "model_version": Param(
            default=SEMANTIC_MODEL_VERSION,
            type="string",
        ),
        "model_name": Param(
            default=SEMANTIC_MODEL_NAME,
            type="string",
        ),
        "container_worker": Param(
            default=DEFAULT_CONTAINER_WORKER,
            type="string",
        ),
        "artifact_registry_base_path": Param(
            default=ARTIFACT_REGISTRY_BASE_PATH,
            type="string",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.11",
        base_dir=BASE_DIR,
        retries=2,
    )

    # No DB is built here any more: the semantic LanceDB is produced/indexed by
    # the `semantic_search_lancedb` job and served from GCS. This step only writes
    # the tiny model_type.json that load_model() reads to pick the SemanticClient.
    write_semantic_metadata = SSHGCEOperator(
        task_id="write_semantic_metadata",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="PYTHONPATH=. uv run cli/create_vector_database.py semantic-metadata",
        dag=dag,
    )

    build_and_push_docker_image = SSHGCEOperator(
        task_id="build_and_push_docker_image",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="PYTHONPATH=. uv run cli/build_and_push_docker_image.py "
        "--experiment-name {{ params.model_version }} "
        "--model-name {{ params.model_name }} "
        "--container-worker {{ params.container_worker }} "
        "--base-serving-container-path {{ params.artifact_registry_base_path }} ",
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="all_done",
    )

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> write_semantic_metadata
        >> build_and_push_docker_image
        >> gce_instance_stop
    )
