import os
from datetime import datetime

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_LINKAGE_DATASET,
    BIGQUERY_ML_PREPROCESSING_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
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

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"artist-linkage-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"
DAG_NAME = "artist_linkage"

# GCS Paths / Filenames
GCS_FOLDER_PATH = f"artist_linkage_{ENV_SHORT_NAME}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
WIKIDATA_STORAGE_BASE_PATH = f"gs://{DATA_GCS_BUCKET_NAME}/dump_wikidata"
ARTISTS_TO_LINK_GCS_FILENAME = "artists_to_link.parquet"
PREPROCESSED_GCS_FILENAME = "preprocessed_artists_to_link.parquet"
ARTIST_LINKED_GCS_FILENAME = "artist_linked.parquet"
WIKIDATA_EXTRACTION_GCS_FILENAME = "wikidata_extraction.parquet"
ARTISTS_MATCHED_ON_WIKIDATA = "artists_matched_on_wikidata.parquet"
ARTISTS_WITH_METADATA_GCS_FILENAME = "artist_linked_with_metadata.parquet"
TEST_SETS_GCS_DIR = "labelled_test_sets"

# BQ Tables
ARTISTS_TO_LINK_TABLE = "artist_name_to_link"
ARTIST_LINK_TABLE = "artist_linked"
default_args = {
    "start_date": datetime(2024, 7, 16),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 5,
}

LINK_FROM_SCRATCH_TASK_ID = "link_from_scratch"
LINK_NEW_PRODUCTS_TO_ARTISTS_TASK_ID = "link_new_products_to_artists"


def _choose_linkage(**context):
    if context["params"]["link_from_scratch"] is True:
        return LINK_FROM_SCRATCH_TASK_ID
    return LINK_NEW_PRODUCTS_TO_ARTISTS_TASK_ID


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Link artists via clustering",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
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
            labels={"job_type": "long_task", "dag_name": DAG_NAME},
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

    gce_instance_stop = DeleteGCEOperator(
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
        import_artists_to_link_to_bucket = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_artists_to_link_to_bucket",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ML_LINKAGE_DATASET,
                        "tableId": ARTISTS_TO_LINK_TABLE,
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
        import_artists_to_link_to_bucket

    with TaskGroup("internal_linkage") as internal_linkage:
        preprocess_data = SSHGCEOperator(
            task_id="preprocess_data",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
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
            command=f"""
             python cluster.py \
            --source-file-path {os.path.join(STORAGE_BASE_PATH, PREPROCESSED_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, ARTIST_LINKED_GCS_FILENAME)}
            """,
        )
        preprocess_data >> artist_linkage
    with TaskGroup("wikidata_matching") as wikidata_matching:
        match_artists_on_wikidata = SSHGCEOperator(
            task_id="match_artists_on_wikidata",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""
             python match_artists_on_wikidata.py \
            --linked-artists-file-path {os.path.join(STORAGE_BASE_PATH, ARTIST_LINKED_GCS_FILENAME)} \
            --wiki-base-path {WIKIDATA_STORAGE_BASE_PATH} \
            --wiki-file-name {WIKIDATA_EXTRACTION_GCS_FILENAME} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_MATCHED_ON_WIKIDATA)}
            """,
        )

        get_wikimedia_commons_license = SSHGCEOperator(
            task_id="get_wikimedia_commons_license",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""
             python get_wikimedia_commons_license.py \
            --artists-matched-on-wikidata {os.path.join(STORAGE_BASE_PATH, ARTISTS_MATCHED_ON_WIKIDATA)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_WITH_METADATA_GCS_FILENAME)}
            """,
        )
        (match_artists_on_wikidata >> get_wikimedia_commons_license)

    load_data_into_artist_linked_table = GCSToBigQueryOperator(
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        task_id="load_data_into_artist_linked_table",
        source_objects=os.path.join(
            GCS_FOLDER_PATH, ARTISTS_WITH_METADATA_GCS_FILENAME
        ),
        destination_project_dataset_table=f"{BIGQUERY_ML_PREPROCESSING_DATASET}.{ARTIST_LINK_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    artist_metrics = (
        SSHGCEOperator(
            task_id="artist_metrics",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=f"""
         python evaluate.py \
        --artists-to-link-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_TO_LINK_GCS_FILENAME)} \
        --linked-artists-file-path {os.path.join(STORAGE_BASE_PATH, ARTISTS_WITH_METADATA_GCS_FILENAME)} \
        --test-sets-dir {os.path.join(STORAGE_BASE_PATH, TEST_SETS_GCS_DIR)} \
        --experiment-name artist_linkage_v1.0_{ENV_SHORT_NAME}
        """,
        ),
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
    (internal_linkage >> wikidata_matching >> load_data_into_artist_linked_table)
    wikidata_matching >> artist_metrics >> gce_instance_stop

    # Link New Products to Artists tasks
    link_new_products_to_artists >> gce_instance_stop
