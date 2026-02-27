from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_FEATURES_DATASET,
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
GCS_FOLDER_PATH = f"item_embedding_{ENV_SHORT_NAME}/{{{{ ds_nodash }}}}"
TEMP_OUTPUT_FILE_NAME = "item_embeddings.parquet"
TEMP_INPUT_FILE_NAME = "item_metadata.parquet"

## BigQuery CONSTANTS
INPUT_DATASET_NAME = f"ml_input_{ENV_SHORT_NAME}"
INPUT_TABLE_NAME = "item_embedding_extraction"
TEMP_INT_TABLE_NAME = "tmp_item_metadata"

OUTPUT_DATASET_NAME = BIGQUERY_ML_FEATURES_DATASET
TEMP_OUTPUT_TABLE_NAME = "item_embedding_tmp"

## DAG CONFIG
DAG_NAME = "item_embedding"
BASE_DIR = "data-gcp/jobs/ml_jobs/item_embedding"
INSTANCE_NAME = "item-embedding"
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-16",
    "prod": "n1-standard-16",
}[ENV_SHORT_NAME]

DEFAULT_ARGS = {
    "start_date": datetime(2025, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

############################################################################
with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    description="Embed items metadata",
    schedule_interval=SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME],
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "batch_size": Param(
            default=100,
            type="integer",
            description="Number of items to process per batch",
        ),
        "embed_all": Param(
            default=False,
            type="boolean",
            description="Whether to embed all items or only the ones that need embedding (to_embed = true in the input table)",
        ),
        "config_file_name": Param(
            default="default",
            type="string",
            description="Name of the configuration file (without .yaml extension)",
        ),
        "instance_type": Param(
            default=INSTANCE_TYPE,
            type="string",
            description="GCE instance type",
        ),
        "instance_name": Param(
            default=INSTANCE_NAME,
            type="string",
            description="GCE instance name",
        ),
        "gpu_type": Param(
            default="nvidia-tesla-t4", enum=INSTANCES_TYPES["gpu"]["name"]
        ),
        "gpu_count": Param(
            default=4 if ENV_SHORT_NAME == "prod" else 0,
            enum=INSTANCES_TYPES["gpu"]["count"],
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        gpu_type="{{ params.gpu_type }}",
        gpu_count="{{ params.gpu_count }}",
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    install_dependencies = InstallDependenciesOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        branch="{{ params.branch }}",
        retries=2,
        dag=dag,
    )

    # Step 1: Select items to embed and save to a temp table in BigQuery
    bigquery_select_items_to_embed = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="bigquery_select_items_to_embed",
        configuration={
            "query": {
                "query": f"""
                    SELECT * FROM `{GCP_PROJECT_ID}.{INPUT_DATASET_NAME}.{INPUT_TABLE_NAME}`
                    {{% if not params.embed_all %}}
                    WHERE to_embed is true
                    {{% endif %}}
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": INPUT_DATASET_NAME,
                    "tableId": TEMP_INT_TABLE_NAME,
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    # Step 2: Export temp table to GCS as a parquet file (to be used as input for the embedding script)
    export_item_metadata_to_gcs = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="export_item_metadata_to_gcs",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": INPUT_DATASET_NAME,
                    "tableId": TEMP_INT_TABLE_NAME,
                },
                "destinationUris": [
                    f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{TEMP_INPUT_FILE_NAME}"
                ],
                "destinationFormat": "PARQUET",
            }
        },
    )

    # Step 3: Run the embedding script on the GCE instance, with the exported parquet file as input,
    # and save the output embeddings as a parquet file in GCS (temp because output parquet only contains to_embed items)
    embed_items = SSHGCEOperator(
        task_id="embed_items",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command=f"""
            uv run python main.py \
                --config-file-name {{{{ params.config_file_name }}}} \
                --batch-size {{{{ params.batch_size }}}} \
                --input-parquet-filename gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{TEMP_INPUT_FILE_NAME} \
                --output-parquet-filename gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{TEMP_OUTPUT_FILE_NAME} \
        """,
    )

    # Step 4: Export the output embeddings from GCS to BigQuery temp table
    # (to be merged with the items table in a separate process after the DAG run)
    export_item_embeddings_to_bigquery = GCSToBigQueryOperator(
        task_id="export_item_embeddings_to_bigquery",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=[f"{GCS_FOLDER_PATH}/{TEMP_OUTPUT_FILE_NAME}"],
        destination_project_dataset_table=f"{OUTPUT_DATASET_NAME}.{TEMP_OUTPUT_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # Step 5: Cleanup temp table in BigQuery
    cleanup_temp_table = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="cleanup_temp_table",
        configuration={
            "query": {
                "query": f"DROP TABLE IF EXISTS `{GCP_PROJECT_ID}.{INPUT_DATASET_NAME}.{TEMP_INT_TABLE_NAME}`",
                "useLegacySql": False,
            }
        },
        trigger_rule="all_done",
    )

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="none_failed",
    )

    stop = EmptyOperator(task_id="stop", dag=dag)

    start >> [gce_instance_start, bigquery_select_items_to_embed]
    gce_instance_start >> install_dependencies
    bigquery_select_items_to_embed >> export_item_metadata_to_gcs
    [
        install_dependencies,
        export_item_metadata_to_gcs,
    ] >> embed_items
    embed_items >> export_item_embeddings_to_bigquery
    export_item_embeddings_to_bigquery >> cleanup_temp_table
    cleanup_temp_table >> gce_instance_delete
    gce_instance_delete >> stop
