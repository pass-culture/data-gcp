import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_FEATURES_DATASET,
    BIGQUERY_ML_LINKAGE_DATASET,
    BIGQUERY_ML_PREPROCESSING_DATASET,
    BIGQUERY_ML_RECOMMENDATION_DATASET,
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    ML_BUCKET_TEMP,
)
from common.operators.bigquery import BigQueryInsertJobOperator
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
DATE = "{{ ts_nodash }}"
DEFAULT_REGION = "europe-west1"
DAG_NAME = "retrieval_vector_build"

# BQ Tables
RECOMMENDABLE_ITEM_TABLE = "recommendable_item"
TWO_TOWER_LAST_ITEM_EMBEDDING_VIEW = "two_tower_last_item_embedding"
TWO_TOWER_LAST_USER_EMBEDDING_VIEW = "two_tower_last_user_embedding"

# Path and filenames
GCS_FOLDER_PATH = f"algo_training_{ENV_SHORT_NAME}/{DAG_NAME}_{DATE}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
TWO_TOWER_LAST_ITEM_EMBEDDING_GCS_DIR = os.path.join(
    STORAGE_BASE_PATH, "two_tower_last_item_embedding_raw"
)
TWO_TOWER_LAST_USER_EMBEDDING_GCS_DIR = os.path.join(
    STORAGE_BASE_PATH, "two_tower_last_user_embedding_raw"
)
RECOMMENDABLE_ITEM_GCS_DIR = os.path.join(STORAGE_BASE_PATH, "recommendable_item_raw")


# Params
gce_params = {
    "base_dir": "data-gcp/jobs/ml_jobs/retrieval_vector",
    "instance_name": f"retrieval-recommendation-build-{ENV_SHORT_NAME}",
    "experiment_name": f"retrieval_recommendation_v1.2_{ENV_SHORT_NAME}",
    "model_name": {
        "dev": "dummy_user_recommendation",
        "stg": "two_towers_user_recommendation",
        "prod": "two_towers_user_recommendation",
    },
    "source_experiment_name": {
        "dev": f"dummy_{ENV_SHORT_NAME}",
        "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
        "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
    },
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-8",
        "prod": "n1-standard-8",
    },
    "container_worker": {"dev": "1", "stg": "1", "prod": "1"},
}

schedule_dict = {"prod": "0 8 * * *", "dev": "0 8 * * *", "stg": "0 8 * * 3"}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Custom Building job",
    schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
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
        "base_dir": Param(
            default=gce_params["base_dir"],
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
        "experiment_name": Param(default=gce_params["experiment_name"], type="string"),
        "model_name": Param(
            default=gce_params["model_name"][ENV_SHORT_NAME], type="string"
        ),
        "container_worker": Param(
            default=gce_params["container_worker"][ENV_SHORT_NAME], type="string"
        ),
        "source_experiment_name": Param(
            default=gce_params["source_experiment_name"][ENV_SHORT_NAME], type="string"
        ),
        "source_run_id": Param(default=".", type="string"),
        "source_artifact_uri": Param(default=".", type="string"),
        "artifact_registry_base_path": Param(
            default=f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/{ENV_SHORT_NAME}",
            type="string",
        ),
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
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
        base_dir="{{ params.base_dir }}",
        retries=2,
    )

    with TaskGroup(
        "import_features_and_embeddings_from_bigquery",
    ) as import_features_and_embeddings_from_bigquery:
        import_recommendable_item_table = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_recommendable_item_table",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ML_RECOMMENDATION_DATASET,
                        "tableId": RECOMMENDABLE_ITEM_TABLE,
                    },
                    "compression": None,
                    "destinationUris": os.path.join(
                        RECOMMENDABLE_ITEM_GCS_DIR, "data_*.parquet"
                    ),
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

        import_two_tower_last_item_embedding = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_two_tower_last_item_embedding",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ML_FEATURES_DATASET,
                        "tableId": TWO_TOWER_LAST_ITEM_EMBEDDING_VIEW,
                    },
                    "compression": None,
                    "destinationUris": os.path.join(
                        TWO_TOWER_LAST_ITEM_EMBEDDING_GCS_DIR,
                        "data_*.parquet",
                    ),
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

        import_two_tower_last_user_embedding = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_two_tower_last_user_embedding",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ML_FEATURES_DATASET,
                        "tableId": TWO_TOWER_LAST_USER_EMBEDDING_VIEW,
                    },
                    "compression": None,
                    "destinationUris": os.path.join(
                        TWO_TOWER_LAST_USER_EMBEDDING_GCS_DIR,
                        "data_*.parquet",
                    ),
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

    if ENV_SHORT_NAME == "dev":
        # dummy deploy
        create_vector_database = SSHGCEOperator(
            task_id="create_vector_database",
            instance_name="{{ params.instance_name }}",
            base_dir="{{ params.base_dir }}",
            command="python create_vector_database.py dummy-database ",
        )
    else:
        create_vector_database = SSHGCEOperator(
            task_id="create_vector_database",
            instance_name="{{ params.instance_name }}",
            base_dir="{{ params.base_dir }}",
            command="python create_vector_database.py default-database "
            "--source-experiment-name {{ params.source_experiment_name }} "
            "--source-artifact-uri {{  params.source_artifact_uri }} "
            "--source-run-id {{ params.source_run_id }} ",
        )

    build_and_push_docker_image = SSHGCEOperator(
        task_id="build_and_push_docker_image",
        instance_name="{{ params.instance_name }}",
        base_dir="{{ params.base_dir }}",
        command="python build_and_push_docker_image.py "
        "--base-serving-container-path {{ params.artifact_registry_base_path }} "
        "--experiment-name {{ params.experiment_name }} "
        "--model-name {{ params.model_name }} "
        "--container-worker {{ params.container_worker }} ",
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    (
        gce_instance_start
        >> fetch_install_code
        >> create_vector_database
        >> build_and_push_docker_image
        >> gce_instance_stop
    )
