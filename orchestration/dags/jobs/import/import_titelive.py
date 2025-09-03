import os
from datetime import datetime, timedelta

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
    BIGQUERY_ML_PREPROCESSING_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
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

BASE_DIR = "data-gcp/jobs/etl_jobs/external/titelive"
DAG_NAME = "import_titelive"

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"import-titelive-{ENV_SHORT_NAME}"
GCS_FOLDER_PATH = f"{DAG_NAME}_{ENV_SHORT_NAME}/{{{{ ds_nodash }}}}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
# TODO: Plug this to the actual metier bucket once we have proper rights.
GCS_THUMB_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/thumb"


OUTPUT_BOOK_TABLE_NAME = "titelive_books"
TITELIVE_WITH_IMAGE_URLS_FILENAME = "titelive_with_image_urls.parquet"

default_args = {
    "owner": "data-team",
    "start_date": datetime(2024, 1, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Export Titelive data pipeline",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value, DAG_TAGS.DE.value, DAG_TAGS.POC.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-4",
            type="enum",
            enum=["n1-standard-1", "n1-standard-2", "n1-standard-4", "n1-standard-8"],
        ),
        "category": Param(
            default="paper",
            enum=["paper", "music"],
        ),
        "custom_min_modified_date": Param(default=None, type=["null", "string"]),
    },
) as dag:
    with TaskGroup("dag_init") as dag_init:
        logging_task = PythonOperator(
            task_id="logging_task",
            python_callable=lambda: print(
                f"Task executed for branch : {dag.params.get('branch')} and instance : {dag.params.get('instance_type')} on env : {ENV_SHORT_NAME}"
            ),
            dag=dag,
        )

    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            instance_type="{{ params.instance_type }}",
            preemptible=False,
            labels={"job_type": "etl_task", "dag_name": DAG_NAME},
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

    with TaskGroup("titelive_extraction") as titelive_extraction:
        extract_offers_task = SSHGCEOperator(
            task_id="extract_new_offers_from_titelive",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""PYTHONPATH=. python scripts/extract_new_offers_from_titelive.py \
                --offer-category {{{{ params.category }}}} \
                --min-modified-date {{{{ params.custom_min_modified_date or macros.ds_add(ds, -1) }}}} \
                --output-file-path {STORAGE_BASE_PATH}/extracted_offers.parquet
                """,
        )

        parse_offers_task = SSHGCEOperator(
            task_id="parse_offers",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""PYTHONPATH=. python scripts/parse_offers.py \
                --min-modified-date {{{{ params.custom_min_modified_date or macros.ds_add(ds, -1) }}}} \
                --input-file-path {STORAGE_BASE_PATH}/extracted_offers.parquet \
                --output-file-path {STORAGE_BASE_PATH}/parsed_offers.parquet
                """,
        )

        upload_images_offers_task = SSHGCEOperator(
            task_id="upload_images_offers_task",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""PYTHONPATH=. python scripts/upload_titelive_images_to_gcs.py \
                --input-parquet-path {STORAGE_BASE_PATH}/parsed_offers.parquet \
                --output-parquet-path {STORAGE_BASE_PATH}/{TITELIVE_WITH_IMAGE_URLS_FILENAME} \
                --gcs-thumb-base-path {GCS_THUMB_BASE_PATH}
                """,
        )

        extract_offers_task >> parse_offers_task >> upload_images_offers_task

    export_data_to_bigquery = GCSToBigQueryOperator(
        task_id=f"load_data_into_{OUTPUT_BOOK_TABLE_NAME}_table",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=os.path.join(GCS_FOLDER_PATH, TITELIVE_WITH_IMAGE_URLS_FILENAME),
        destination_project_dataset_table=f"{BIGQUERY_ML_PREPROCESSING_DATASET}.{OUTPUT_BOOK_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE, trigger_rule="none_failed"
    )

    # Task dependencies
    (
        dag_init
        >> vm_init
        >> titelive_extraction
        >> export_data_to_bigquery
        >> gce_instance_stop
    )
