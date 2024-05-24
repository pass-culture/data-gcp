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
from dependencies.ml.linkage.import_artists import PARAMS

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"link-offers-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/record_linkage"
STORAGE_PATH = f"{DATA_GCS_BUCKET_NAME}/link_artists"

default_args = {
    "start_date": datetime(2024, 5, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
}

with DAG(
    "link_artists",
    default_args=default_args,
    description="Link artists via clustering",
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

    export_file_uri = f"gs://{STORAGE_PATH}/artists_to_match.parquet"
    export_bq_to_gcs = BigQueryInsertJobOperator(
        task_id=f"{PARAMS['destination_table']}_to_bucket",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": PARAMS["destination_table"],
                },
                "compression": None,
                "destinationUris": export_file_uri,
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    (logging_task >> data_collect >> export_bq_to_gcs)
