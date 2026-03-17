from datetime import datetime, timedelta
from itertools import chain

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    INSTANCES_TYPES,
    ML_BUCKET_TEMP,
)
from common.operators.bigquery import BigQueryInsertJobOperator
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

from jobs.crons import SCHEDULE_DICT

###########################################################################
## GCS CONSTANTS
GCS_FOLDER_PATH = "edito_semantic_search/item_embeddings_{{ ts_nodash }}"
GCS_OUTPUT_FILENAME = "data-*.parquet"

## BigQuery CONSTANTS
INPUT_DATASET_NAME = f"ml_feat_{ENV_SHORT_NAME}"
INPUT_TABLE_NAME = "item_embedding_refactor"

## DAG CONFIG
DAG_NAME = "edito_semantic_search"
BASE_DIR = "data-gcp/jobs/ml_jobs/edito_semantic_search"
INSTANCE_NAME = "create-lancedb-embeddings"
INSTANCE_TYPE = {
    "dev": "n1-standard-4",
    "stg": "n1-standard-4",
    "prod": "n1-standard-4",
}[ENV_SHORT_NAME]

DEFAULT_ARGS = {
    "start_date": datetime(2025, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

############################################################################
DAG_DOC = """
"""

with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    description="Create LanceDB with item embeddings",
    doc_md=DAG_DOC,
    schedule_interval=SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME],
    catchup=False,
    dagrun_timeout=timedelta(hours=12),
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
            enum=list(chain(*INSTANCES_TYPES["cpu"].values())),
            description="GCE instance type",
        ),
        "instance_name": Param(
            default=INSTANCE_NAME,
            type="string",
            description="GCE instance name",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        labels={"job_type": "extra_long_ml", "dag_name": DAG_NAME},
    )

    install_dependencies = InstallDependenciesOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        branch="{{ params.branch }}",
        retries=2,
    )

    # Step 2: Export temp table to GCS as a parquet file (to be used as input for the embedding script)
    export_item_embeddings_to_gcs = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="export_item_embeddings_to_gcs",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": INPUT_DATASET_NAME,
                    "tableId": INPUT_TABLE_NAME,
                },
                "destinationUris": [
                    f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{GCS_OUTPUT_FILENAME}"
                ],
                "destinationFormat": "PARQUET",
            }
        },
    )

    create_lancedb = SSHGCEOperator(
        task_id="create_lancedb",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="""
            uv run python main.py \

        """,
        deferrable=False,
    )

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="all_done",  # always delete the VM, even on upstream failure
    )

    stop = EmptyOperator(task_id="stop")

    (
        start
        >> gce_instance_start
        >> install_dependencies
        >> export_item_embeddings_to_gcs
        >> create_lancedb
        >> gce_instance_delete
        >> stop
    )
