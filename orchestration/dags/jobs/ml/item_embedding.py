from datetime import datetime, timedelta
from itertools import chain

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
    GCE_ZONES,
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
GCS_FOLDER_PATH = f"item_embedding_{ENV_SHORT_NAME}/{{{{ ts_nodash }}}}"
INPUT_FOLDER = "input_item_metadata"
OUTPUT_FOLDER = "output_item_embeddings"
TEMP_OUTPUT_FILE_NAME = "item_embeddings_*.parquet"
TEMP_INPUT_FILE_NAME = "item_metadata_*.parquet"

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
GCE_ZONE_TEMPLATE = "{{ params.gce_zone }}"
INSTANCE_TYPE = {
    "dev": "n1-standard-4",
    "stg": "g2-standard-48",
    "prod": "g2-standard-48",
}[ENV_SHORT_NAME]

DEFAULT_ARGS = {
    "start_date": datetime(2025, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

############################################################################
DAG_DOC = """
    ### Item embedding DAG

    #### Parameters:
    *embed_all* : whether to embed all items or only the ones that need embedding (to_embed = true in the input table)
    *config_file_name* : name of the configuration file (without .yaml extension) in the `config` folder, which contains the vector configurations and other parameters for the embedding process.
    *instance_type* : GCE instance type to use for embedding. For L4 GPU instances, make sure to select a compatible machine type with the number of GPUs you want to use. Check hint below.
    *instance_name* : name of the GCE instance to create for embedding.
    *gpu_type* : If you decide to embedd all the catalogue, we highly recommend to use 4 L4 GPUs, in europe-west1-c (to avoid stockout issues in europe-west1-b). If you have a smaller catalogue or if you want to embed only the new items, you can use 4 T4 GPU, which is more widely available across zones.
    *gpu_count* : number of GPUs to use for embedding (only applicable for GPU instance types). Make sure to select a machine type that supports the number of GPUs you want to use.
    *gce_zone* : GCE zone to use for embedding. Only europe-west1-c and europe-west1-b have L4 GPUs. europe-west1-d has T4 GPUs. Stockout are very frequent.
    *provisioning_model* : STANDARD (default) requests the GPU immediately and fails on stockout. FLEX_START uses Dynamic Workload Scheduler (DWS): instead of failing, the request is queued until GPU capacity frees up (queue held for up to *request_valid_for_duration*, hard-capped at 2h by GCP). Uses preemptible quota. Best for the full-catalogue L4 run given frequent stockouts.
    *max_run_duration* / *request_valid_for_duration* : FLEX_START only. See parameter descriptions.
    *reservation_name* : Consume a specific Compute Engine reservation (e.g. the one auto-created by a future reservation on its start date). When set, use provisioning_model=STANDARD (incompatible with FLEX_START) and make sure instance_type/gpu_type/gpu_count/gce_zone match the reservation exactly.

    *Hint:* For L4 GPUs, make sure to select a compatible g2 machine. The Number of L4 GPUs you can attach to a G2 depends on its RAM.
    Here is the breakdown:
            "g2-standard-4/8/12/16/32": 1 L4,
            "g2-standard-24": 2 L4s,
            "g2-standard-48": 4 L4s,
            "g2-standard-96": 8 L4s,
    ⚠️ caution: frequent stockouts on L4 GPUs, especially in europe-west1-b, try europe-west1-c or europe-west1-d if you encounter stockouts.
"""

with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    description="Embed items metadata",
    doc_md=DAG_DOC,
    schedule=SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME],
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
            enum=list(chain(*INSTANCES_TYPES["cpu"].values())),
            description="GCE instance type",
        ),
        "instance_name": Param(
            default=INSTANCE_NAME,
            type="string",
            description="GCE instance name",
        ),
        "gpu_type": Param(
            default="nvidia-tesla-t4" if ENV_SHORT_NAME == "dev" else "nvidia-l4",
            enum=INSTANCES_TYPES["gpu"]["name"],
        ),
        "gpu_count": Param(
            default=1 if ENV_SHORT_NAME == "dev" else 4,
            enum=INSTANCES_TYPES["gpu"]["count"],
            description="""Number of GPUs to use for embedding
                        (only applicable for GPU instance types).
                        """,
        ),
        "gce_zone": Param(default="europe-west1-c", enum=GCE_ZONES),
        "provisioning_model": Param(
            default="FLEX_START",
            enum=["STANDARD", "FLEX_START"],
            description="""VM provisioning model. STANDARD requests capacity
                        immediately (fails on stockout). FLEX_START uses Dynamic
                        Workload Scheduler (DWS) to queue the GPU request until
                        capacity is available (queue held for up to
                        request_valid_for_duration, max 2h).""",
        ),
        "max_run_duration": Param(
            default="12h",
            type="string",
            description="""(FLEX_START only) Max VM run duration before it is
                        auto-deleted. Accepts e.g. '12h', '1d2h', or seconds.
                        Max 7 days.""",
        ),
        "request_valid_for_duration": Param(
            default="2h",
            type="string",
            description="""(FLEX_START only) How long DWS holds the request in
                        queue while the VM is PENDING. Accepts e.g. '2h', '90m'.
                        Must be 0 or between 90s and 2h.""",
        ),
        "reservation_name": Param(
            default="",
            type="string",
            description="""Name of a specific Compute Engine reservation to
                        consume (e.g. the reservation auto-created by a future
                        reservation on its start date). When set, the VM targets
                        this reservation via SPECIFIC_RESERVATION and requires
                        provisioning_model=STANDARD (incompatible with
                        FLEX_START). The instance_type, gpu_type, gpu_count and
                        gce_zone must match the reservation exactly. Leave empty
                        to not target any reservation.""",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        gpu_type="{{ params.gpu_type }}",
        gpu_count="{{ params.gpu_count }}",
        gce_zone=GCE_ZONE_TEMPLATE,
        labels={"job_type": "extra_long_ml", "dag_name": DAG_NAME},
        provisioning_model="{{ params.provisioning_model }}",
        max_run_duration="{{ params.max_run_duration }}",
        request_valid_for_duration="{{ params.request_valid_for_duration }}",
        reservation_name="{{ params.reservation_name }}",
        # Defer while a FLEX_START request sits queued so the worker slot is freed.
        deferrable=True,
        # Cover the max 2h DWS queue wait plus provisioning/boot margin.
        execution_timeout=timedelta(hours=3),
        retries=3,
    )

    install_dependencies = InstallDependenciesOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        branch="{{ params.branch }}",
        gce_zone=GCE_ZONE_TEMPLATE,
        retries=2,
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
                    f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{INPUT_FOLDER}/{TEMP_INPUT_FILE_NAME}"
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
        gce_zone=GCE_ZONE_TEMPLATE,
        command=f"""
            uv run python main.py \
                --config-file-name {{{{ params.config_file_name }}}} \
                --input-parquets-folder-path gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{INPUT_FOLDER} \
                --output-parquets-folder-path gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{OUTPUT_FOLDER} \
        """,
        deferrable=True,
    )

    # Step 4: Export the output embeddings from GCS to BigQuery temp table
    # (to be merged with the items table in a separate process after the DAG run)
    export_item_embeddings_to_bigquery = GCSToBigQueryOperator(
        task_id="export_item_embeddings_to_bigquery",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=[f"{GCS_FOLDER_PATH}/{OUTPUT_FOLDER}/*.parquet"],
        destination_project_dataset_table=f"{OUTPUT_DATASET_NAME}.{TEMP_OUTPUT_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        gce_zone=GCE_ZONE_TEMPLATE,  # delete in the zone the VM was created in
        trigger_rule="all_done",  # always delete the VM, even on upstream failure
    )

    stop = EmptyOperator(task_id="stop")

    start >> [gce_instance_start, bigquery_select_items_to_embed]
    gce_instance_start >> install_dependencies
    bigquery_select_items_to_embed >> export_item_metadata_to_gcs
    [
        install_dependencies,
        export_item_metadata_to_gcs,
    ] >> embed_items
    embed_items >> export_item_embeddings_to_bigquery
    export_item_embeddings_to_bigquery >> gce_instance_delete
    gce_instance_delete >> stop
