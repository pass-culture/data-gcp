from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.biquery import BigQueryInsertJobOperator, bigquery_job_task
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from dependencies.ml.linkage.artist_linkage_on_test_set import (
    PARAMS as ARTIST_LINKAGE_ON_TEST_SET_PARAMS,
)
from dependencies.ml.linkage.import_artists import PARAMS as IMPORT_ARTISTS_PARAMS

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"link-artists-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"
STORAGE_PATH = f"{DATA_GCS_BUCKET_NAME}/link_artists"
INPUT_GCS_PATH = f"gs://{STORAGE_PATH}/artists_to_match.parquet"
PREPROCESSED_GCS_PATH = f"gs://{STORAGE_PATH}/preprocessed_artists_to_match.parquet"
OUTPUT_GCS_PATH = f"gs://{STORAGE_PATH}/matched_artists.parquet"

# Test set paths
IMPORT_TEST_SET_GCS_BASE_PATH = f"gs://{STORAGE_PATH}/labelled_test_sets"
TEST_SET_BQ_TABLE = "test_set"
LINKED_ARTISTS_IN_TEST_SET_PATH = (
    f"gs://{STORAGE_PATH}/linked_artists_in_test_set.parquet"
)
ARTIST_METRICS_PATH = f"gs://{STORAGE_PATH}/artist_metrics.parquet"

default_args = {
    "start_date": datetime(2024, 5, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
}


with DAG(
    "link_artists",
    default_args=default_args,
    description="Link artists via clustering",
    schedule_interval=None,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-32",
            type="string",
        ),
    },
) as dag:
    # GCE
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        labels={"job_type": "ml"},
        preemptible=False,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        python_version="3.10",
        command="{{ params.branch }}",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
    )

    logging_task = PythonOperator(
        task_id="logging_task",
        python_callable=lambda: print(
            f"Task executed for branch : {dag.params.get('branch')} and instance : {dag.params.get('instance_type')} on env : {ENV_SHORT_NAME}"
        ),
        dag=dag,
    )

    # Artist Linkage
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
                "destinationUris": INPUT_GCS_PATH,
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    collect_test_sets_into_bq = GCSToBigQueryOperator(
        task_id="import_test_sets_in_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            f"{IMPORT_TEST_SET_GCS_BASE_PATH.split(f'{DATA_GCS_BUCKET_NAME}/')[-1]}/*.parquet"
        ],
        destination_project_dataset_table=f"{BIGQUERY_TMP_DATASET}.{TEST_SET_BQ_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    preprocess_data = SSHGCEOperator(
        task_id="preprocess_data",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python preprocess.py \
        --source-file-path {INPUT_GCS_PATH} \
        --output-file-path {PREPROCESSED_GCS_PATH}
        """,
    )

    artist_linkage = SSHGCEOperator(
        task_id="artist_linkage",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python cluster.py \
        --source-file-path {PREPROCESSED_GCS_PATH} \
        --output-file-path {OUTPUT_GCS_PATH}
        """,
    )

    load_artist_linkage_to_bigquery = GCSToBigQueryOperator(
        bucket=DATA_GCS_BUCKET_NAME,
        task_id="load_artist_linkage_to_bigquery",
        source_objects=OUTPUT_GCS_PATH.split(f"{DATA_GCS_BUCKET_NAME}/")[-1],
        destination_project_dataset_table=f"{BIGQUERY_TMP_DATASET}.matched_artists",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # Metrics
    artist_linkage_on_test_set = bigquery_job_task(
        dag,
        f"create_bq_table_{ARTIST_LINKAGE_ON_TEST_SET_PARAMS['destination_table']}",
        ARTIST_LINKAGE_ON_TEST_SET_PARAMS,
    )

    artist_linkage_on_test_set_to_gcs = BigQueryInsertJobOperator(
        task_id=f"{ARTIST_LINKAGE_ON_TEST_SET_PARAMS['destination_table']}_to_bucket",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": ARTIST_LINKAGE_ON_TEST_SET_PARAMS["destination_table"],
                },
                "compression": None,
                "destinationUris": LINKED_ARTISTS_IN_TEST_SET_PATH,
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    artist_metrics = SSHGCEOperator(
        task_id="artist_metrics",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python evaluate.py \
        --input-file-path {LINKED_ARTISTS_IN_TEST_SET_PATH} \
        --output-file-path {ARTIST_METRICS_PATH}
        """,
    )

    artist_metrics_to_bigquery = GCSToBigQueryOperator(
        bucket=DATA_GCS_BUCKET_NAME,
        task_id="artist_metrics_to_bigquery",
        source_objects=ARTIST_METRICS_PATH.split(f"{DATA_GCS_BUCKET_NAME}/")[-1],
        destination_project_dataset_table=f"{BIGQUERY_TMP_DATASET}.artist_metrics",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    (logging_task >> gce_instance_start >> fetch_code >> install_dependencies)
    (logging_task >> data_collect >> export_input_bq_to_gcs)
    (logging_task >> collect_test_sets_into_bq)

    (
        [export_input_bq_to_gcs, install_dependencies]
        >> preprocess_data
        >> artist_linkage
        >> load_artist_linkage_to_bigquery
    )

    (
        [load_artist_linkage_to_bigquery, collect_test_sets_into_bq]
        >> artist_linkage_on_test_set
        >> artist_linkage_on_test_set_to_gcs
        >> artist_metrics
        >> artist_metrics_to_bigquery
        >> gce_instance_stop
    )
