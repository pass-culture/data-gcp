import os
from datetime import datetime
from itertools import chain

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_LINKAGE_DATASET,
    BIGQUERY_ML_PREPROCESSING_DATASET,
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    INSTANCES_TYPES,
    ML_BUCKET_TEMP,
    DagBaseConfig,
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

# GCE
GCS_FOLDER_PATH = f"event_linkage_{ENV_SHORT_NAME}/{{{{ ts_nodash }}}}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
DEFAULT_ARGS = {
    "start_date": datetime(2024, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 5,
}


class GCEConfig(DagBaseConfig):
    _INSTANCE_TYPES = {
        "dev": "n1-standard-2",
        "stg": "n1-standard-8",
        "prod": "n1-standard-8",
    }
    instance_type: str = _INSTANCE_TYPES[ENV_SHORT_NAME]
    instance_name: str = f"event-linkage-{ENV_SHORT_NAME}"


class GCSConfig(DagBaseConfig):
    ml_bucket_temp: str = ML_BUCKET_TEMP
    gcs_folder_path: str = GCS_FOLDER_PATH
    event_offer_to_link_gcs_path: str = (
        f"{STORAGE_BASE_PATH}/00_event_offer_to_link.parquet"
    )
    offers_with_embedded_images_gcs_path: str = (
        f"{STORAGE_BASE_PATH}/01_offers_with_embedded_images.parquet"
    )
    similarities_gcs_path: str = f"{STORAGE_BASE_PATH}/02_similarities.parquet"
    delta_event_series_gcs_path: str = (
        f"{STORAGE_BASE_PATH}/03_delta_event_series.parquet"
    )
    delta_event_series_offer_link_gcs_path: str = (
        f"{STORAGE_BASE_PATH}/03_delta_event_series_offer_link.parquet"
    )
    applicative_event_series_offer_link_gcs_path: str = (
        f"{STORAGE_BASE_PATH}/applicative_event_series_offer_link.parquet"
    )


class BigQueryConfig(DagBaseConfig):
    event_offer_to_link_table: str = "event_offer_to_link"
    delta_event_series_table: str = "delta_event_series"
    delta_event_series_offer_link_table: str = "delta_event_series_offer_link"
    applicative_event_series_offer_link_table: str = (
        "applicative_database_event_series_offer_link"
    )
    linkage_dataset: str = BIGQUERY_ML_LINKAGE_DATASET
    preprocessing_dataset: str = BIGQUERY_ML_PREPROCESSING_DATASET
    raw_dataset: str = BIGQUERY_RAW_DATASET


class DagConfig(DagBaseConfig):
    dag_id: str = "event_linkage"
    base_dir: str = "data-gcp/jobs/ml_jobs/event_linkage"
    storage_path: str = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
    gce: GCEConfig = GCEConfig()
    gcs: GCSConfig = GCSConfig()
    bq: BigQueryConfig = BigQueryConfig()


# Tables to load into BQ after processing
DAG_CONFIG = DagConfig()
GCS_TO_DELTA_TABLES = [
    {
        "filename": DAG_CONFIG.gcs.delta_event_series_gcs_path.split("/")[-1],
        "table_id": DAG_CONFIG.bq.delta_event_series_table,
    },
    {
        "filename": DAG_CONFIG.gcs.delta_event_series_offer_link_gcs_path.split("/")[
            -1
        ],
        "table_id": DAG_CONFIG.bq.delta_event_series_offer_link_table,
    },
]

with DAG(
    DAG_CONFIG.dag_id,
    default_args=DEFAULT_ARGS,
    description="Link offer events between them and create event series objects.",
    schedule=get_airflow_schedule(SCHEDULE_DICT[DAG_CONFIG.dag_id][ENV_SHORT_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=DAG_CONFIG.gce.instance_type,
            enum=list(chain(*INSTANCES_TYPES["cpu"].values())),
        ),
        "linkage_mode": Param(
            default="incremental",
            enum=["incremental", "from_scratch"],
            type="string",
        ),
    },
) as dag:
    with TaskGroup("dag_init") as dag_init:
        logging_task = PythonOperator(
            task_id="logging_task",
            python_callable=lambda: print(
                f"Task executed for branch : {dag.params.get('branch')} and instance : {dag.params.get('instance_type')} on env : {ENV_SHORT_NAME}"
            ),
        )

    with TaskGroup("import_data_to_gcs") as import_data_to_gcs:
        import_event_offer_to_link_to_gcs = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_event_offer_to_link_to_gcs",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": DAG_CONFIG.bq.linkage_dataset,
                        "tableId": DAG_CONFIG.bq.event_offer_to_link_table,
                    },
                    "compression": None,
                    "destinationUris": DAG_CONFIG.gcs.event_offer_to_link_gcs_path,
                    "destinationFormat": "PARQUET",
                }
            },
        )

        import_applicative_event_series_offer_link_to_gcs = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_applicative_event_series_offer_link_to_gcs",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": DAG_CONFIG.bq.raw_dataset,
                        "tableId": DAG_CONFIG.bq.applicative_event_series_offer_link_table,
                    },
                    "compression": None,
                    "destinationUris": DAG_CONFIG.gcs.applicative_event_series_offer_link_gcs_path,
                    "destinationFormat": "PARQUET",
                }
            },
        )

    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=DAG_CONFIG.gce.instance_name,
            instance_type="{{ params.instance_type }}",
            gpu_type="nvidia-tesla-t4",
            gpu_count=1,
            preemptible=False,
            labels={"dag_name": DAG_CONFIG.dag_id},
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=DAG_CONFIG.gce.instance_name,
            branch="{{ params.branch }}",
            python_version="3.12",
            base_dir=DAG_CONFIG.base_dir,
            retries=2,
        )
        gce_instance_start >> fetch_install_code

    embed_offer_images = SSHGCEOperator(
        task_id="embed_offer_images",
        instance_name=DAG_CONFIG.gce.instance_name,
        base_dir=DAG_CONFIG.base_dir,
        command=f"""
             uv run python cli/1_embed_offer_images.py \
                --offer-event-filepath {DAG_CONFIG.gcs.event_offer_to_link_gcs_path} \
                --output-filepath {DAG_CONFIG.gcs.offers_with_embedded_images_gcs_path}
            """,
    )

    compute_similarities = SSHGCEOperator(
        task_id="compute_similarities",
        instance_name=DAG_CONFIG.gce.instance_name,
        base_dir=DAG_CONFIG.base_dir,
        command=f"""
             uv run python cli/2_compute_similarities.py \
                --offer-event-with-embeddings-filepath {DAG_CONFIG.gcs.offers_with_embedded_images_gcs_path} \
                --output-filepath {DAG_CONFIG.gcs.similarities_gcs_path}
            """,
    )

    # The linkage_mode param is used to determine whether to re-cluster all offers from scratch or to perform an incremental update.
    create_delta_event_series = SSHGCEOperator(
        task_id="create_delta_event_series",
        instance_name=DAG_CONFIG.gce.instance_name,
        base_dir=DAG_CONFIG.base_dir,
        command="uv run python cli/3_create_delta_event_tables.py "
        f"--offer-event-filepath {DAG_CONFIG.gcs.event_offer_to_link_gcs_path} "
        f"--similarities-filepath {DAG_CONFIG.gcs.similarities_gcs_path} "
        f"--event-series-offer-link-filepath {DAG_CONFIG.gcs.applicative_event_series_offer_link_gcs_path} "
        f"--delta-events-filepath {DAG_CONFIG.gcs.delta_event_series_gcs_path} "
        f"--delta-event-offer-link-filepath {DAG_CONFIG.gcs.delta_event_series_offer_link_gcs_path} "
        "{%- if params['linkage_mode'] == 'from_scratch'  %} --from-scratch{%- endif %}",
    )

    with TaskGroup(
        "load_delta_event_series_tables_to_bq",
        default_args={"trigger_rule": "none_failed_min_one_success"},
    ) as load_delta_event_series_tables_to_bq:
        for table_data in GCS_TO_DELTA_TABLES:
            GCSToBigQueryOperator(
                task_id=f"load_data_into_{table_data['table_id']}_table",
                project_id=GCP_PROJECT_ID,
                bucket=DAG_CONFIG.gcs.ml_bucket_temp,
                source_objects=os.path.join(
                    DAG_CONFIG.gcs.gcs_folder_path, table_data["filename"]
                ),
                destination_project_dataset_table=f"{DAG_CONFIG.bq.preprocessing_dataset}.{table_data['table_id']}",
                source_format="PARQUET",
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
            )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name=DAG_CONFIG.gce.instance_name,
        trigger_rule="none_failed",
    )

    (
        dag_init
        >> import_data_to_gcs
        >> vm_init
        >> embed_offer_images
        >> compute_similarities
        >> create_delta_event_series
        >> load_delta_event_series_tables_to_bq
        >> gce_instance_stop
    )
