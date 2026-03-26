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
GCS_FOLDER_PATH = "semantic_search_lancedb"
INPUT_FOLDER = "item_embeddings_{{ ts_nodash }}"
GCS_OUTPUT_FILENAME = "data-*.parquet"

## BigQuery CONSTANTS
INPUT_DATASET_NAME = f"ml_feat_{ENV_SHORT_NAME}"
INPUT_TABLE_NAME = "item_embedding_refactor"
VECTOR_COLUMN_NAME = "semantic_content_sts"

## LanceDB CONSTANTS
LANCEDB_URI = f"gs://{ML_BUCKET_TEMP}/lancedb/{ENV_SHORT_NAME}"
LANCEDB_TABLE = "item_embeddings"

## DAG CONFIG
DAG_NAME = "semantic_search_lancedb"
BASE_DIR = "data-gcp/jobs/ml_jobs/semantic_search_lancedb"
INSTANCE_NAME = "semantic-search-lancedb"
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
        "vector_embedding_column_name": Param(
            default=VECTOR_COLUMN_NAME,
            type="string",
            description="""Name of the column containing the vector embedding in BigQuery.
            Make sure it exists in the input table.""",
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
                    f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{INPUT_FOLDER}/{GCS_OUTPUT_FILENAME}"
                ],
                "destinationFormat": "PARQUET",
            }
        },
    )

    create_lancedb = SSHGCEOperator(
        task_id="create_lancedb",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command=f"""
            uv run python main.py \
                --gcs-embedding-parquet-file gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{INPUT_FOLDER} \
                --lancedb-uri {LANCEDB_URI} \
                --lancedb-table {LANCEDB_TABLE} \
                --batch-size 10000 \
                --vector-column-name {{{{ params.vector_embedding_column_name }}}}
        """,
        deferrable=False,
    )

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="all_done",
    )

    stop = EmptyOperator(task_id="stop")

    (start >> [gce_instance_start, export_item_embeddings_to_gcs])
    gce_instance_start >> install_dependencies
    [install_dependencies, export_item_embeddings_to_gcs] >> create_lancedb
    create_lancedb >> gce_instance_delete >> stop
