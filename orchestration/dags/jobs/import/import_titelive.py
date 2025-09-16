import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
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
GCS_THUMB_BASE_PATH = {
    "prod": f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/thumb",  # For prod, we use a different bucket path because metier bucket is too sensitive
    "stg": "gs://passculture-metier-ehp-staging-assets-fine-grained/thumbs",
    "dev": "gs://passculture-metier-ehp-testing-assets-fine-grained/thumbs",
}[ENV_SHORT_NAME]
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
            enum=["n1-standard-1", "n1-standard-2", "n1-standard-4", "n1-standard-8"],
        ),
        "category": Param(
            default="paper",
            enum=["paper", "music"],
        ),
        "custom_min_modified_date": Param(default=None, type=["null", "string"]),
        "upload_images": Param(default=False, type="boolean"),
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
        extract_products_task = SSHGCEOperator(
            task_id="extract_new_products_from_titelive",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""PYTHONPATH=. python scripts/extract_new_products_from_titelive.py \
                --product-category {{{{ params.category }}}} \
                --min-modified-date {{{{ params.custom_min_modified_date or macros.ds_add(ds, -1) }}}} \
                --output-file-path {STORAGE_BASE_PATH}/extracted_products.parquet
                """,
        )

        parse_products_task = SSHGCEOperator(
            task_id="parse_products",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""PYTHONPATH=. python scripts/parse_products.py \
                --min-modified-date {{{{ params.custom_min_modified_date or macros.ds_add(ds, -1) }}}} \
                --input-file-path {STORAGE_BASE_PATH}/extracted_products.parquet \
                --output-file-path {STORAGE_BASE_PATH}/parsed_products.parquet
                """,
        )

        extract_products_task >> parse_products_task

    # Upload images task (outside TaskGroup to avoid cycles)
    upload_images_products_task = SSHGCEOperator(
        task_id="upload_images_products_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""PYTHONPATH=. python scripts/upload_titelive_images_to_gcs.py \
            --input-parquet-path {STORAGE_BASE_PATH}/parsed_products.parquet \
            --output-parquet-path {STORAGE_BASE_PATH}/{TITELIVE_WITH_IMAGE_URLS_FILENAME} \
            --gcs-thumb-base-path {GCS_THUMB_BASE_PATH}
            """,
    )

    # Branch decision based on upload_images parameter
    def decide_upload_images_branch(**context):
        upload_images = context["params"].get("upload_images", True)
        if upload_images:
            return "upload_images_products_task"
        else:
            return "export_data_without_images"

    branch_task = BranchPythonOperator(
        task_id="decide_upload_images",
        python_callable=decide_upload_images_branch,
    )

    # Create conditional export tasks
    export_data_with_images = GCSToBigQueryOperator(
        task_id="export_data_with_images",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=os.path.join(GCS_FOLDER_PATH, TITELIVE_WITH_IMAGE_URLS_FILENAME),
        destination_project_dataset_table=f"{BIGQUERY_ML_PREPROCESSING_DATASET}.{OUTPUT_BOOK_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    export_data_without_images = GCSToBigQueryOperator(
        task_id="export_data_without_images",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=os.path.join(GCS_FOLDER_PATH, "parsed_products.parquet"),
        destination_project_dataset_table=f"{BIGQUERY_ML_PREPROCESSING_DATASET}.{OUTPUT_BOOK_TABLE_NAME}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # Join task to continue after either branch
    join_task = EmptyOperator(
        task_id="join_branches", trigger_rule="none_failed_min_one_success"
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE, trigger_rule="none_failed"
    )

    # Task dependencies
    dag_init >> vm_init >> titelive_extraction >> branch_task

    # Branch paths
    branch_task >> upload_images_products_task >> export_data_with_images >> join_task
    branch_task >> export_data_without_images >> join_task

    # Final dependency
    join_task >> gce_instance_stop
