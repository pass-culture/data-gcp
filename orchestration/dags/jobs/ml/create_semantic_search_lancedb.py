from datetime import datetime, timedelta
from itertools import chain

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_FEATURES_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
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
## GCS TEMP CONSTANTS
INPUT_GCS_FOLDER_URI = (
    f"gs://{ML_BUCKET_TEMP}/semantic_search_lancedb/item_embeddings_{{{{ ts_nodash }}}}"
)
INPUT_FILENAME = "item_embeddings_*.parquet"

## BigQuery CONSTANTS
INPUT_BQ_TABLE_NAME = "item_embedding_refactor"
DEFAULT_VECTOR_COLUMN_NAME = "semantic_content_sts"

## GCS LanceDB CONSTANTS
LANCEDB_GCS_URI = f"gs://{DATA_GCS_BUCKET_NAME}/semantic_search_lancedb/"
LANCEDB_TABLE = "item_embeddings"

## DAG CONFIG
DAG_ID = "semantic_search_lancedb"
BASE_DIR = "data-gcp/jobs/ml_jobs/semantic_search_lancedb"
INSTANCE_NAME = "semantic-search-lancedb"
INSTANCE_TYPE = "n1-standard-4"


DEFAULT_ARGS = {
    "start_date": datetime(2025, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

############################################################################
DAG_DOC = f"""
This DAG creates a LanceDB table with item embeddings for semantic search. It performs the following steps:
1. Starts a GCE instance.
2. Exports item embeddings from `ml_feat_<env>_item_embedding_refactor` BigQuery table to Parquet files in GCS.
3. Creates LanceDB table indexed on the vector_embedding_column_name. Stored in GCS at gs://{DATA_GCS_BUCKET_NAME}/semantic_search_lancedb/{ENV_SHORT_NAME}.

Parameters:
- vector_embedding_column_name: Name of the column containing the vector embeddings in the BigQuery table (default: 'semantic_content_sts').
Make sure this column exists in `ml_feat_<env>_item_embedding_refactor` .
"""

with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Create LanceDB with item embeddings",
    doc_md=DAG_DOC,
    schedule_interval=SCHEDULE_DICT[DAG_ID][ENV_SHORT_NAME],
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
            default=DEFAULT_VECTOR_COLUMN_NAME,
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
        labels={"job_type": "extra_long_ml", "dag_name": DAG_ID},
    )

    install_dependencies = InstallDependenciesOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        branch="{{ params.branch }}",
        retries=2,
        python_version="3.11",
    )

    export_item_embeddings_to_gcs = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="export_item_embeddings_to_gcs",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_FEATURES_DATASET,
                    "tableId": INPUT_BQ_TABLE_NAME,
                },
                "destinationUris": [f"{INPUT_GCS_FOLDER_URI}/{INPUT_FILENAME}"],
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
                --gcs-embedding-parquet-file {INPUT_GCS_FOLDER_URI} \
                --lancedb-uri {LANCEDB_GCS_URI} \
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
