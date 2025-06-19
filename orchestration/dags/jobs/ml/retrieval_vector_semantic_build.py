from datetime import datetime, timedelta

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_RETRIEVAL_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MLFLOW_BUCKET_NAME,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

DATE = "{{ ts_nodash }}"
DAG_NAME = "retrieval_semantic_vector_build"
# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/retrieval_vector_{ENV_SHORT_NAME}/semantic_{DATE}/{DATE}_item_embbedding_data",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/retrieval_vector/",
    "EXPERIMENT_NAME": f"retrieval_semantic_vector_v0.1_{ENV_SHORT_NAME}",
    "MODEL_NAME": f"v1.1_{ENV_SHORT_NAME}",
}

gce_params = {
    "instance_name": f"retrieval-semantic-vector-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-8",
        "prod": "n1-standard-16",
    },
    "container_worker": {"dev": "1", "stg": "1", "prod": "1"},
}

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

schedule_dict = {"prod": "0 4 * * *", "dev": "0 6 * * *", "stg": "0 6 * * 3"}

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
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"],
            type="string",
        ),
        "experiment_name": Param(
            default=dag_config["EXPERIMENT_NAME"],
            type="string",
        ),
        "model_name": Param(
            default=dag_config["MODEL_NAME"],
            type="string",
        ),
        "container_worker": Param(
            default=gce_params["container_worker"][ENV_SHORT_NAME],
            type="string",
        ),
        "artifact_registry_base_path": Param(
            default=f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/{ENV_SHORT_NAME}",
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

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
        base_dir=dag_config["BASE_DIR"],
        retries=2,
    )

    export_bq = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="store_item_embbedding_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_RETRIEVAL_DATASET,
                    "tableId": "semantic_vector_item_embedding",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    semantic_retrieval_build = SSHGCEOperator(
        task_id="semantic_retrieval_build",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python create_vector_database.py semantic-database "
        f"--source-gs-path {dag_config['STORAGE_PATH']} ",
        dag=dag,
    )

    build_and_push_docker_image = SSHGCEOperator(
        task_id="build_and_push_docker_image",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python build_and_push_docker_image.py "
        "--base-serving-container-path {{ params.artifact_registry_base_path }} "
        "--experiment-name {{ params.experiment_name }} "
        "--model-name {{ params.model_name }} "
        "--container-worker {{ params.container_worker }} ",
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> semantic_retrieval_build
        >> build_and_push_docker_image
        >> gce_instance_stop
    )
    (start >> export_bq >> semantic_retrieval_build)
