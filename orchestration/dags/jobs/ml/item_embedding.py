import os
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

## GCS
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/item_embedding"
OUTPUT_FILE_NAME = "item_embeddings.parquet"
INPUT_FILE_NAME = "item_metadata.parquet"

## BigQuery
INPUT_DATASET_NAME = f"ml_input_{ENV_SHORT_NAME}"
OUTPUT_TABLE_NAME = (
    "item_embedding_tmp"  ## TODO: change table name when refactor is done
)
INPUT_TABLE_NAME = "item_embedding_extraction"

DATE = "{{ ts_nodash }}"
DAG_NAME = "item_embedding"
# Environment variables to export before running commands
dag_config = {
    "BASE_DIR": "data-gcp/jobs/ml_jobs/item_embedding",
}

# Params
INSTANCE_NAME = f"item-embedding-{ENV_SHORT_NAME}"
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-16",
    "prod": "n1-standard-16",
}[ENV_SHORT_NAME]

default_args = {
    "start_date": datetime(2025, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Embed items metadata",
    schedule_interval=SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME],
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        # Infrastructure params (can be overridden at runtime)
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
            description="Whether to embed all items or only the ones that need embedding",
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
        base_dir=dag_config["BASE_DIR"],
        branch="{{ params.branch }}",
        retries=2,
        dag=dag,
    )

    # Step 1: Run query and save to temp table
    run_query = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="run_query",
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
                    "datasetId": f"sandbox_{ENV_SHORT_NAME}",
                    "tableId": "tmp_item_metadata",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    # Step 2: Export temp table to GCS
    export_to_gcs = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="export_to_gcs",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": f"sandbox_{ENV_SHORT_NAME}",
                    "tableId": "tmp_item_metadata",
                },
                "compression": None,
                "destinationUris": os.path.join(STORAGE_BASE_PATH, INPUT_FILE_NAME),
                "destinationFormat": "PARQUET",
            }
        },
    )

    embed_items = SSHGCEOperator(
        task_id="embed_items",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command=f"""
            uv run python main.py \
                --config-file-name {{{{ params.config_file_name }}}} \
                --batch-size {{{{ params.batch_size }}}} \
                --input-parquet-filename {os.path.join(STORAGE_BASE_PATH, INPUT_FILE_NAME)} \
                --output-parquet-filename {os.path.join(STORAGE_BASE_PATH, OUTPUT_FILE_NAME)} \
        """,
    )

    export_item_embeddings_to_bigquery = GCSToBigQueryOperator(
        task_id="export_item_embeddings_to_bigquery",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=os.path.join(STORAGE_BASE_PATH, OUTPUT_FILE_NAME),
        destination_project_dataset_table=f"{BIGQUERY_ML_FEATURES_DATASET}.{OUTPUT_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="none_failed",
    )

    (
        start
        >> gce_instance_start
        >> install_dependencies
        >> run_query
        >> export_to_gcs
        >> embed_items
        >> export_item_embeddings_to_bigquery
        >> gce_instance_delete
    )
