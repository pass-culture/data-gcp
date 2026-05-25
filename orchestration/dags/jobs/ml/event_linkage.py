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
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

# GCE
DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"event-linkage-{ENV_SHORT_NAME}"
DEFAULT_CPU_INSTANCE = "n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-8"


# Local Paths
BASE_DIR = "data-gcp/jobs/ml_jobs/event_linkage"

# Airflow
DAG_ID = "event_linkage"
default_args = {
    "start_date": datetime(2024, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 5,
}

# BQ Tables
EVENT_OFFER_TO_LINK_TABLE = "event_offer_to_link"
DELTA_EVENT_SERIES_TABLE = "delta_event_series"
DELTA_EVENT_SERIES_OFFER_LINKS_TABLE = "delta_event_series_offer_links"


# GCS Paths / Filenames
GCS_FOLDER_PATH = f"event_linkage_{ENV_SHORT_NAME}/{{{{ ts_nodash }}}}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
EVENT_OFFER_TO_LINK_GCS_PATH = f"{STORAGE_BASE_PATH}/00_event_offer_to_link.parquet"
OFFERS_WITH_EMBEDDED_IMAGES_GCS_PATH = (
    f"{STORAGE_BASE_PATH}/01_offers_with_embedded_images.parquet"
)
SIMILARITIES_GCS_PATH = f"{STORAGE_BASE_PATH}/02_similarities.parquet"
DELTA_EVENT_SERIES_GCS_PATH = f"{STORAGE_BASE_PATH}/03_delta_event_series.parquet"
DELTA_EVENT_SERIES_OFFER_LINKS_GCS_PATH = (
    f"{STORAGE_BASE_PATH}/03_delta_event_series_offer_links.parquet"
)

# Tables to load into BQ after processing
GCS_TO_DELTA_TABLES = [
    {
        "filename": DELTA_EVENT_SERIES_GCS_PATH.split("/")[-1],
        "table_id": DELTA_EVENT_SERIES_TABLE,
    },
    {
        "filename": DELTA_EVENT_SERIES_OFFER_LINKS_GCS_PATH.split("/")[-1],
        "table_id": DELTA_EVENT_SERIES_OFFER_LINKS_TABLE,
    },
]

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Link offer events between them and create event series objects.",
    schedule=get_airflow_schedule(SCHEDULE_DICT[DAG_ID]),
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
            default=DEFAULT_CPU_INSTANCE,
            enum=list(chain(*INSTANCES_TYPES["cpu"].values())),
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

    import_event_offer_to_link_to_gcs = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="import_event_offer_to_link_to_gcs",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_LINKAGE_DATASET,
                    "tableId": EVENT_OFFER_TO_LINK_TABLE,
                },
                "compression": None,
                "destinationUris": EVENT_OFFER_TO_LINK_GCS_PATH,
                "destinationFormat": "PARQUET",
            }
        },
    )

    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            instance_type="{{ params.instance_type }}",
            gpu_type="nvidia-tesla-t4",
            gpu_count=1,
            preemptible=False,
            labels={"dag_name": DAG_ID},
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=GCE_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.12",
            base_dir=BASE_DIR,
            retries=2,
        )
        gce_instance_start >> fetch_install_code

    embed_offer_images = SSHGCEOperator(
        task_id="embed_offer_images",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run python cli/1_embed_offer_images.py \
                --offer-event-filepath {EVENT_OFFER_TO_LINK_GCS_PATH} \
                --output-filepath {OFFERS_WITH_EMBEDDED_IMAGES_GCS_PATH}
            """,
    )

    compute_similarities = SSHGCEOperator(
        task_id="compute_similarities",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run python cli/2_compute_similarities.py \
                --offer-event-with-embeddings-filepath {OFFERS_WITH_EMBEDDED_IMAGES_GCS_PATH} \
                --output-filepath {SIMILARITIES_GCS_PATH}
            """,
    )

    create_delta_event_series = SSHGCEOperator(
        task_id="create_delta_event_series",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""             uv run python cli/3_create_delta_event_tables.py \
                --offer-event-filepath {EVENT_OFFER_TO_LINK_GCS_PATH} \
                --similarities-filepath {SIMILARITIES_GCS_PATH} \
                --delta-events-filepath {DELTA_EVENT_SERIES_GCS_PATH} \
                --delta-event-offer-links-filepath {DELTA_EVENT_SERIES_OFFER_LINKS_GCS_PATH}
            """,
    )

    with TaskGroup(
        "load_delta_event_series_tables_to_bq",
        default_args={"trigger_rule": "none_failed_min_one_success"},
    ) as load_delta_event_series_tables_to_bq:
        for table_data in GCS_TO_DELTA_TABLES:
            GCSToBigQueryOperator(
                task_id=f"load_data_into_{table_data['table_id']}_table",
                project_id=GCP_PROJECT_ID,
                bucket=ML_BUCKET_TEMP,
                source_objects=os.path.join(GCS_FOLDER_PATH, table_data["filename"]),
                destination_project_dataset_table=f"{BIGQUERY_ML_PREPROCESSING_DATASET}.{table_data['table_id']}",
                source_format="PARQUET",
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
            )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE, trigger_rule="none_failed"
    )

    (
        dag_init
        >> import_event_offer_to_link_to_gcs
        >> vm_init
        >> embed_offer_images
        >> compute_similarities
        >> create_delta_event_series
        >> load_delta_event_series_tables_to_bq
        >> gce_instance_stop
    )
