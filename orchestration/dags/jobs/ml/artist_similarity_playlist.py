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
    BIGQUERY_ML_INPUT_DATASET,
    BIGQUERY_ML_PREPROCESSING_DATASET,
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
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

# GCE
DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"artist-similarity-{ENV_SHORT_NAME}"
DEFAULT_CPU_INSTANCE = "n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-8"


# Local Paths
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"

# Airflow
DAG_ID = "artist_similarity_playlist"
default_args = {
    "start_date": datetime(2024, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 5,
}

# BQ Tables
ARTIST_METADATA_WITH_EMBEDDING_TABLE = "artist_metadata_with_embedding"
SIMILART_ARTIST_TABLE = "similar_artist"


# GCS Paths / Filenames
GCS_FOLDER_PATH = f"artist_linkage_{ENV_SHORT_NAME}/{{{{ ds_nodash }}}}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
WIKIDATA_STORAGE_BASE_PATH = f"gs://{DATA_GCS_BUCKET_NAME}/dump_wikidata"
WIKIDATA_EXTRACTION_GCS_FILENAME = "wikidata_extraction.parquet"
ARTIST_WITH_BIOGRAPHY_GCS_PATH = (
    f"{STORAGE_BASE_PATH}/artist_metadata_with_embedding.parquet"
)
ARTIST_WITH_ENCODED_BIOGRAPHY_GCS_PATH = (
    f"{STORAGE_BASE_PATH}/artist_with_encoded_biography.parquet"
)
SIMILART_ARTIST_GCS_PATH = f"{STORAGE_BASE_PATH}/similar_artist.parquet"

with DAG(
    DAG_ID,
    default_args=default_args,
    description="Compute similar artists based on Two Tower and Semantic Embeddings.",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_ID]),
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

    import_artist_metadata_with_embedding_to_gcs = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="import_artist_metadata_with_embedding_to_gcs",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_INPUT_DATASET,
                    "tableId": ARTIST_METADATA_WITH_EMBEDDING_TABLE,
                },
                "compression": None,
                "destinationUris": ARTIST_WITH_BIOGRAPHY_GCS_PATH,
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
            python_version="3.10",
            base_dir=BASE_DIR,
            retries=2,
        )
        gce_instance_start >> fetch_install_code

    encode_artist_biographies = SSHGCEOperator(
        task_id="encode_artist_biographies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run python cli/encode_artist_biographies.py \
                --artist-with-biography-file-path {ARTIST_WITH_BIOGRAPHY_GCS_PATH} \
                --wiki-base-path {WIKIDATA_STORAGE_BASE_PATH} \
                --wiki-file-name {WIKIDATA_EXTRACTION_GCS_FILENAME} \
                --output-file-path {ARTIST_WITH_ENCODED_BIOGRAPHY_GCS_PATH}
            """,
    )

    create_similar_artist_parquet = SSHGCEOperator(
        task_id="create_similar_artist_parquet",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run python cli/create_similar_artist_parquet.py \
                --artist-with-embeddings-file-path {ARTIST_WITH_ENCODED_BIOGRAPHY_GCS_PATH} \
                --output-file-path {SIMILART_ARTIST_GCS_PATH}
            """,
    )

    load_similar_artist_parquet_to_bq = GCSToBigQueryOperator(
        task_id="load_similar_artist_parquet_to_bq",
        project_id=GCP_PROJECT_ID,
        bucket=ML_BUCKET_TEMP,
        source_objects=[f"{GCS_FOLDER_PATH}/similar_artist.parquet"],
        destination_project_dataset_table=f"{BIGQUERY_ML_PREPROCESSING_DATASET}.{SIMILART_ARTIST_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE, trigger_rule="none_failed"
    )

    (
        dag_init
        >> import_artist_metadata_with_embedding_to_gcs
        >> vm_init
        >> encode_artist_biographies
        >> create_similar_artist_parquet
        >> load_similar_artist_parquet_to_bq
        >> gce_instance_stop
    )
