from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET,
    BIGQUERY_ML_RECOMMENDATION_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    ML_BUCKET_TEMP,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

# Ariflow params
DATE = "{{ ts_nodash }}"
DAG_NAME = "build_and_push_graph_retrieval_api"
default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# GCS Paths / Filenames
BASE_DIR = "data-gcp/jobs/ml_jobs/retrieval_vector/"
GCS_FOLDER_PATH = f"{DAG_NAME}_{ENV_SHORT_NAME}/{{{{ ds_nodash }}}}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"

# GCE
INSTANCE_NAME = f"{DAG_NAME.replace('_', '-')}-{ENV_SHORT_NAME}"
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-8",
    "prod": "n1-standard-16",
}[ENV_SHORT_NAME]
DEFAULT_CONTAINER_WORKER = "1"

# Registry
ARTIFACT_REGISTRY_BASE_PATH = f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/{ENV_SHORT_NAME}"
GRAPH_RETRIEVAL_MODEL_NAME = "metapath2vec"
GRAPH_RETRIEVAL_MODEL_VERSION = f"graph_retrieval_recommendation_v0.1_{ENV_SHORT_NAME}"

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Custom training job",
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
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
            default=GRAPH_RETRIEVAL_MODEL_VERSION,
            type="string",
        ),
        "model_name": Param(
            default=GRAPH_RETRIEVAL_MODEL_NAME,
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

    with TaskGroup(
        "import_data_from_bq_to_gcs", tooltip="Data Preparation"
    ) as import_data_from_bq_to_gcs:
        BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_recommendable_item_data",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ML_RECOMMENDATION_DATASET,
                        "tableId": "recommendable_item",
                    },
                    "compression": None,
                    "destinationUris": f"{STORAGE_BASE_PATH}/raw_recommendable_item/data-*.parquet",
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

        BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_graph_embedding_data",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET,
                        "tableId": "graph_embedding",
                    },
                    "compression": None,
                    "destinationUris": f"{STORAGE_BASE_PATH}/raw_graph_embedding/data-*.parquet",
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_DIR,
        retries=2,
    )

    create_graph_retrieval_database = SSHGCEOperator(
        task_id="create_graph_retrieval_database",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="python create_vector_database.py graph-database "
        f"--recommendable-item-gs-path {STORAGE_BASE_PATH}/raw_recommendable_item "
        f"--graph-embedding-gs-path {STORAGE_BASE_PATH}/raw_graph_embedding ",
        dag=dag,
    )

    build_and_push_docker_image = SSHGCEOperator(
        task_id="build_and_push_docker_image",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="python build_and_push_docker_image.py "
        "--experiment-name {{ params.model_version }} "
        "--model-name {{ params.model_name }} "
        "--container-worker {{ params.container_worker }} "
        "--base-serving-container-path {{ params.artifact_registry_base_path }} ",
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    (start >> import_data_from_bq_to_gcs >> create_graph_retrieval_database)
    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> create_graph_retrieval_database
        >> build_and_push_docker_image
        >> gce_instance_stop
    )
