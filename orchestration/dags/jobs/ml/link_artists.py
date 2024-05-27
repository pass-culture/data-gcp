from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
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
from dependencies.ml.linkage.import_artists import PARAMS

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"link-artists-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"
STORAGE_PATH = f"{DATA_GCS_BUCKET_NAME}/link_artists"
INPUT_GCS_PATH = f"gs://{STORAGE_PATH}/artists_to_match.parquet"
OUTPUT_GCS_PATH = f"gs://{STORAGE_PATH}/matched_artists.parquet"


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
    logging_task = PythonOperator(
        task_id="logging_task",
        python_callable=lambda: print(
            f"Task executed for branch : {dag.params.get('branch')} and instance : {dag.params.get('instance_type')} on env : {ENV_SHORT_NAME}"
        ),
        dag=dag,
    )

    data_collect = bigquery_job_task(
        dag, f"create_bq_table_{PARAMS['destination_table']}", PARAMS
    )

    export_input_bq_to_gcs = BigQueryInsertJobOperator(
        task_id=f"{PARAMS['destination_table']}_to_bucket",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": PARAMS["destination_table"],
                },
                "compression": None,
                "destinationUris": INPUT_GCS_PATH,
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        labels={"job_type": "ml"},
        preemptible=False,
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

    artist_linkage = SSHGCEOperator(
        task_id="artist_linkage",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python main.py \
        --source-file-path {INPUT_GCS_PATH} \
        --output-file-path {OUTPUT_GCS_PATH}
        """,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        logging_task
        >> data_collect
        >> export_input_bq_to_gcs
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> artist_linkage
        >> gce_instance_stop
    )
