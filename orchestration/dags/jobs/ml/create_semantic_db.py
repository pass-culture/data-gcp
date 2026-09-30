from datetime import datetime, timedelta
from itertools import chain

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_INPUT_DATASET,
    BIGQUERY_ML_RECOMMENDATION_DATASET,
    BIGQUERY_ML_SEMANTIC_EMBEDDING_DATASET,
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
    f"gs://{ML_BUCKET_TEMP}/semantic_db_creation/item_embeddings_{{{{ ts_nodash }}}}"
)
INPUT_FILENAME = "item_embeddings_*.parquet"

## BigQuery CONSTANTS
ITEM_EMBEDDING_TABLE = "all_items_metadata"
ITEM_METADATA_TABLE = "item_metadata"
RECOMMENDABLE_ITEM_TABLE = "recommendable_item"
DEFAULT_VECTOR_COLUMN_NAME = "all_items_metadata_embedding"

## GCS LanceDB CONSTANTS
LANCEDB_GCS_URI = f"gs://{DATA_GCS_BUCKET_NAME}/semantic_search_lancedb/"
# Table name the retrieval_vector SemanticClient opens (open_table("items")).
LANCEDB_TABLE = "items"

## DAG CONFIG
DAG_ID = "create_semantic_db"
BASE_DIR = "data-gcp/jobs/ml_jobs/semantic_db_creation"
INSTANCE_NAME = "semantic-db-creation"
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
2. Exports item embeddings from `ml_semantic_embedding_<env>.all_items_metadata` BigQuery table to Parquet files in GCS.
3. Creates LanceDB table indexed on the vector_embedding_column_name. Stored in GCS at gs://{DATA_GCS_BUCKET_NAME}/semantic_search_lancedb/{ENV_SHORT_NAME}.

Parameters:
- vector_embedding_column_name: Name of the column containing the vector embeddings in the BigQuery table (default: 'all_items_metadata_embedding').
Make sure this column exists in `ml_semantic_embedding_<env>.all_items_metadata` .
"""

with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Create LanceDB with item embeddings",
    doc_md=DAG_DOC,
    schedule=SCHEDULE_DICT[DAG_ID][ENV_SHORT_NAME],
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
            "query": {
                "query": f"""
                    EXPORT DATA OPTIONS(
                        uri='{INPUT_GCS_FOLDER_URI}/{INPUT_FILENAME}',
                        format='PARQUET',
                        overwrite=true
                    ) AS
                    SELECT
                        emb.item_id,
                        emb.{{{{ params.vector_embedding_column_name }}}},
                        im.offer_name,
                        im.offer_description,
                        ri.category,
                        ri.subcategory_id,
                        ri.search_group_name,
                        ri.topic_id,
                        ri.cluster_id,
                        ri.is_geolocated,
                        ri.gtl_id,
                        ri.gtl_l3,
                        ri.gtl_l4,
                        ri.booking_number,
                        ri.booking_number_last_7_days,
                        ri.booking_number_last_14_days,
                        ri.booking_number_last_28_days,
                        ri.booking_number_desc,
                        ri.total_offers,
                        CAST(ri.stock_price AS FLOAT64) AS stock_price,
                        -- `offer_creation_date` / `stock_beginning_date` are BQ DATE
                        -- columns: exported as unix-epoch seconds (INT64, the native
                        -- return type of `UNIX_SECONDS`), matching what the
                        -- co-reservation / graph retrieval actually serves (e.g.
                        -- `"offer_creation_date": 1727053696`, no decimal — see
                        -- `retrieval_vector/src/vector_database.py`'s `_to_ts`). Left
                        -- as a raw DATE, the value leaks as an exotic string
                        -- (RFC 2822 date) once it crosses the gRPC/JSON boundary,
                        -- breaking the recommendation API's Pydantic `datetime`
                        -- parsing. `IFNULL(..., 0)` mirrors `_to_ts`'s exception
                        -- fallback.
                        IFNULL(
                            UNIX_SECONDS(TIMESTAMP(ri.offer_creation_date)), 0
                        ) AS offer_creation_date,
                        IFNULL(
                            UNIX_SECONDS(TIMESTAMP(ri.stock_beginning_date)), 0
                        ) AS stock_beginning_date,
                        CAST(ri.semantic_emb_mean AS FLOAT64) AS semantic_emb_mean,
                        ri.example_offer_id,
                        ri.example_offer_name,
                        ri.example_venue_id,
                        CAST(ri.example_venue_latitude AS FLOAT64)
                            AS example_venue_latitude,
                        CAST(ri.example_venue_longitude AS FLOAT64)
                            AS example_venue_longitude
                    FROM `{GCP_PROJECT_ID}.{BIGQUERY_ML_SEMANTIC_EMBEDDING_DATASET}.{ITEM_EMBEDDING_TABLE}` AS emb
                    INNER JOIN `{GCP_PROJECT_ID}.{BIGQUERY_ML_INPUT_DATASET}.{ITEM_METADATA_TABLE}` AS im
                        ON emb.item_id = im.item_id
                    LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_ML_RECOMMENDATION_DATASET}.{RECOMMENDABLE_ITEM_TABLE}` AS ri
                        ON emb.item_id = ri.item_id
                """,
                "useLegacySql": False,
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
