import os
from datetime import datetime

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MLFLOW_BUCKET_NAME,
)
from common.operators.bigquery import BigQueryInsertJobOperator, bigquery_job_task
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule, sparkql_health_check
from dependencies.ml.linkage.create_artist_alias_table import (
    PARAMS as CREATE_ARTIST_ALIAS_TABLE_PARAMS,
)
from dependencies.ml.linkage.create_artist_table import (
    PARAMS as CREATE_ARTIST_TABLE_PARAMS,
)
from dependencies.ml.linkage.create_product_artist_link_table import (
    PARAMS as CREATE_PRODUCT_ARTIST_LINK_TABLE_PARAMS,
)
from dependencies.ml.linkage.import_artists import PARAMS as IMPORT_ARTISTS_PARAMS

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup

ML_DAG_TAG = "ML"
VM_DAG_TAG = "VM"

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"artist-linkage-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"
SCHEDULE_CRON = "0 3 * * 1"

# GCS Paths / Filenames
GCS_FOLDER_PATH = f"artist_linkage_{ENV_SHORT_NAME}"
STORAGE_BASE_PATH = f"gs://{MLFLOW_BUCKET_NAME}/{GCS_FOLDER_PATH}"
ARTISTS_TO_LINK_GCS_FILENAME = "artists_to_link.parquet"
PREPROCESSED_GCS_FILENAME = "preprocessed_artists_to_link.parquet"
LINKED_ARTISTS_GCS_FILENAME = "linked_artists.parquet"
WIKIDATA_EXTRACTION_GCS_FILENAME = "wikidata_extraction.parquet"
ARTISTS_MATCHED_ON_WIKIDATA = "artists_matched_on_wikidata.parquet"
ARTISTS_WITH_METADATA_GCS_FILENAME = "linked_artists_with_metadata.parquet"
TEST_SETS_GCS_DIR = "labelled_test_sets"
GCE_INSTALLER = "uv"

# BQ Output Tables
LINKED_ARTISTS_BQ_TABLE = "linked_artists"
QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"

default_args = {
    "start_date": datetime(2024, 7, 16),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 5,
}

LINK_FROM_SCRATCH_TASK_ID = "link_from_scratch"
LINK_NEW_PRODUCTS_TO_ARTISTS_TASK_ID = "link_new_products_to_artists"


def _choose_linkage(**context):
    if context["params"]["link_from_scratch"] is True:
        return LINK_FROM_SCRATCH_TASK_ID
    return LINK_NEW_PRODUCTS_TO_ARTISTS_TASK_ID


with DAG(
    "artist_linkage",
    default_args=default_args,
    description="Link artists via clustering",
    schedule_interval=get_airflow_schedule(SCHEDULE_CRON),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[ML_DAG_TAG, VM_DAG_TAG],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-8",
            type="string",
        ),
        "link_from_scratch": Param(
            default=True,
            type="boolean",
        ),
    },
) as dag:
    with TaskGroup("dag_init") as dag_init:
        # check fribourg uni serveur availability

        health_check_task = PythonOperator(
            task_id="health_check_task",
            python_callable=sparkql_health_check,
            op_args=[QLEVER_ENDPOINT],
            dag=dag,
        )
        logging_task = PythonOperator(
            task_id="logging_task",
            python_callable=lambda: print(
                f"Task executed for branch : {dag.params.get('branch')} and instance : {dag.params.get('instance_type')} on env : {ENV_SHORT_NAME}"
            ),
            dag=dag,
        )
        health_check_task >> logging_task

    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            instance_type="{{ params.instance_type }}",
            labels={"job_type": "ml"},
            preemptible=False,
        )
        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=GCE_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=BASE_DIR,
            retries=2,
        )
        gce_instance_start >> fetch_install_code

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE, trigger_rule="none_failed"
    )

    choose_linkage = BranchPythonOperator(
        task_id="choose_path",
        python_callable=_choose_linkage,
        provide_context=True,
        dag=dag,
    )
    link_from_scratch = DummyOperator(task_id=LINK_FROM_SCRATCH_TASK_ID)
    link_new_products_to_artists = DummyOperator(
        task_id=LINK_NEW_PRODUCTS_TO_ARTISTS_TASK_ID
    )

    # Artist Linkage
    with TaskGroup("data_collection") as collect:
        data_collect = bigquery_job_task(
            dag,
            f"create_bq_table_{IMPORT_ARTISTS_PARAMS['destination_table']}",
            IMPORT_ARTISTS_PARAMS,
        )

        export_input_bq_to_gcs = BigQueryInsertJobOperator(
            task_id=f"{IMPORT_ARTISTS_PARAMS['destination_table']}_to_bucket",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": IMPORT_ARTISTS_PARAMS["destination_table"],
                    },
                    "compression": None,
                    "destinationUris": os.path.join(
                        STORAGE_BASE_PATH, ARTISTS_TO_LINK_GCS_FILENAME
                    ),
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )
        data_collect >> export_input_bq_to_gcs

    with TaskGroup("internal_linkage") as internal_linkage:
        preprocess_data = SSHGCEOperator(
            task_id="preprocess_data",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            installer=GCE_INSTALLER,
            command=f"""
             python preprocess.py \
            --source-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_TO_LINK_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, PREPROCESSED_GCS_FILENAME)}
            """,
        )

        artist_linkage = SSHGCEOperator(
            task_id="artist_linkage",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            installer=GCE_INSTALLER,
            command=f"""
             python cluster.py \
            --source-file-path {os.path.join(STORAGE_BASE_PATH, PREPROCESSED_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, LINKED_ARTISTS_GCS_FILENAME)}
            """,
        )
        preprocess_data >> artist_linkage
    with TaskGroup("wikidata_matching") as wikidata_matching:
        extract_from_wikidata = SSHGCEOperator(
            task_id="extract_from_wikidata",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            installer=GCE_INSTALLER,
            command=f"""
             python extract_from_wikidata.py \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, WIKIDATA_EXTRACTION_GCS_FILENAME)}
            """,
        )

        match_artists_on_wikidata = SSHGCEOperator(
            task_id="match_artists_on_wikidata",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            installer=GCE_INSTALLER,
            command=f"""
             python match_artists_on_wikidata.py \
            --linked-artists-file-path {os.path.join(STORAGE_BASE_PATH, LINKED_ARTISTS_GCS_FILENAME)} \
            --wiki-file-path {os.path.join(STORAGE_BASE_PATH, WIKIDATA_EXTRACTION_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_MATCHED_ON_WIKIDATA)}
            """,
        )

        get_wikimedia_commons_license = SSHGCEOperator(
            task_id="get_wikimedia_commons_license",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            installer=GCE_INSTALLER,
            command=f"""
             python get_wikimedia_commons_license.py \
            --artists-matched-on-wikidata {os.path.join(STORAGE_BASE_PATH, ARTISTS_MATCHED_ON_WIKIDATA)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_WITH_METADATA_GCS_FILENAME)}
            """,
        )
        (
            extract_from_wikidata
            >> match_artists_on_wikidata
            >> get_wikimedia_commons_license
        )

    load_data_into_linked_artists_table = GCSToBigQueryOperator(
        bucket=MLFLOW_BUCKET_NAME,
        task_id="load_data_into_linked_artists_table",
        source_objects=os.path.join(
            GCS_FOLDER_PATH, ARTISTS_WITH_METADATA_GCS_FILENAME
        ),
        destination_project_dataset_table=f"{BIGQUERY_TMP_DATASET}.{LINKED_ARTISTS_BQ_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    artist_metrics = (
        SSHGCEOperator(
            task_id="artist_metrics",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            installer=GCE_INSTALLER,
            command=f"""
         python evaluate.py \
        --artists-to-link-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_TO_LINK_GCS_FILENAME)} \
        --linked-artists-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_WITH_METADATA_GCS_FILENAME)} \
        --test-sets-dir {os.path.join(STORAGE_BASE_PATH, TEST_SETS_GCS_DIR)} \
        --experiment-name artist_linkage_v1.0_{ENV_SHORT_NAME}
        """,
        ),
    )

    with TaskGroup("export_data") as export_data:
        create_artist_table = bigquery_job_task(
            dag,
            f"create_bq_table_{CREATE_ARTIST_TABLE_PARAMS['destination_table']}",
            CREATE_ARTIST_TABLE_PARAMS,
        )

        create_product_artist_link_table = bigquery_job_task(
            dag,
            f"create_bq_table_{CREATE_PRODUCT_ARTIST_LINK_TABLE_PARAMS['destination_table']}",
            CREATE_PRODUCT_ARTIST_LINK_TABLE_PARAMS,
        )

        create_artist_alias_table = bigquery_job_task(
            dag,
            f"create_bq_table_{CREATE_ARTIST_ALIAS_TABLE_PARAMS['destination_table']}",
            CREATE_ARTIST_ALIAS_TABLE_PARAMS,
        )

    # Common tasks
    (
        dag_init
        >> vm_init
        >> choose_linkage
        >> [link_from_scratch, link_new_products_to_artists]
    )

    # Link From Scratch tasks
    link_from_scratch >> collect >> internal_linkage
    link_from_scratch >> [internal_linkage, wikidata_matching]
    (
        internal_linkage
        >> wikidata_matching
        >> load_data_into_linked_artists_table
        >> export_data
    )
    wikidata_matching >> artist_metrics >> gce_instance_stop

    # Link New Products to Artists tasks
    link_new_products_to_artists >> gce_instance_stop
