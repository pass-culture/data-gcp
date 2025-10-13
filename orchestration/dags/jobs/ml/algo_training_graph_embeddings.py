import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET,
    BIGQUERY_ML_PREPROCESSING_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    INSTANCES_TYPES,
    ML_BUCKET_TEMP,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

# Airflow DAG definition
DATE = "{{ ts_nodash }}"
DAG_NAME = "algo_training_graph_embeddings"
DEFAULT_ARGS = {
    "start_date": datetime(2023, 5, 9),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# GCE
INSTANCE_NAME = f"algo-training-graph-embeddings-{ENV_SHORT_NAME}"
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-16",
    "prod": "n1-standard-16",
}[ENV_SHORT_NAME]

# Path and filenames
GCS_FOLDER_PATH = f"algo_training_{ENV_SHORT_NAME}/{DAG_NAME}_{DATE}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
BASE_DIR = "data-gcp/jobs/ml_jobs/graph_recommendation"
EMBEDDINGS_FILENAME = "embeddings.parquet"

# BQ Tables
INPUT_TABLE_NAME = "item_with_metadata_to_embed"
EMBEDDING_TABLE_NAME = "graph_embedding"

with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    description="Training job for building embeddings based on the metadatas graph",
    schedule_interval=get_airflow_schedule(
        SCHEDULE_DICT.get(DAG_NAME).get(ENV_SHORT_NAME)
    ),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1200),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    render_template_as_native_obj=True,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=INSTANCE_TYPE, enum=INSTANCES_TYPES["cpu"]["standard"]
        ),
        "instance_name": Param(default=INSTANCE_NAME, type="string"),
        "gpu_type": Param(
            default="nvidia-tesla-t4", enum=INSTANCES_TYPES["gpu"]["name"]
        ),
        "gpu_count": Param(default=1, enum=INSTANCES_TYPES["gpu"]["count"]),
        "run_name": Param(default="default", type=["string", "null"]),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    import_offer_as_parquet = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="import_offer_as_parquet",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET,
                    "tableId": INPUT_TABLE_NAME,
                },
                "compression": None,
                "destinationUris": f"{STORAGE_BASE_PATH}/raw_input/data-*.parquet",
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
        gpu_type="{{ params.gpu_type }}",
        gpu_count="{{ params.gpu_count }}",
        labels={"job_type": "long_ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.13",
        base_dir=BASE_DIR,
        retries=2,
    )

    train = SSHGCEOperator(
        task_id="train",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python -m scripts.cli train-metapath2vec "
        f"{STORAGE_BASE_PATH}/raw_input "
        f"--output-embeddings {STORAGE_BASE_PATH}/{EMBEDDINGS_FILENAME}",
        dag=dag,
    )

    upload_embeddings_to_bigquery = GCSToBigQueryOperator(
        task_id=f"load_embeddings_into_{EMBEDDING_TABLE_NAME}_table",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=os.path.join(GCS_FOLDER_PATH, EMBEDDINGS_FILENAME),
        destination_project_dataset_table=f"{BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET}.{EMBEDDING_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    (
        start
        >> import_offer_as_parquet
        >> gce_instance_start
        >> fetch_install_code
        >> train
        >> upload_embeddings_to_bigquery
        >> gce_instance_stop
    )
