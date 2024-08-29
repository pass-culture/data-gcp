import time
from datetime import datetime, timedelta

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.utils import get_airflow_schedule
from google.cloud import storage

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

QPI_ANSWERS_SCHEMA = [
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "submitted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {
        "name": "answers",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "question_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "answer_ids", "type": "STRING", "mode": "REPEATED"},
        ],
    },
]
TYPEFORM_FUNCTION_NAME = "qpi_import_" + ENV_SHORT_NAME
QPI_ANSWERS_TABLE = "qpi_answers_v4"

default_args = {
    "start_date": datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def verify_folder():
    today = time.strftime("%Y%m%d")
    storage_client = storage.Client()
    bucket = storage_client.bucket(DATA_GCS_BUCKET_NAME)
    name = f"QPI_exports/qpi_answers_{today}/"
    stats = bucket.list_blobs(prefix=name)
    blob_list = []
    for s in stats:
        blob_list.append(s)
    if len(blob_list) > 0:
        return "Files"
    else:
        return "Empty"


with DAG(
    "import_qpi_answers_v1",
    default_args=default_args,
    description="Importing new data from QPI every day.",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
) as dag:
    start = DummyOperator(task_id="start")

    checking_folder_QPI = BranchPythonOperator(
        task_id="checking_folder_QPI", python_callable=verify_folder
    )
    file = DummyOperator(task_id="Files")
    empty = DummyOperator(task_id="Empty")

    import_historical_answers_to_bigquery = GCSToBigQueryOperator(
        task_id="import_historical_answers_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=["QPI_historical/qpi_answers_historical_*.parquet"],
        destination_project_dataset_table="{{ bigquery_raw_dataset }}.qpi_answers_historical",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # it fetches the file corresponding to the initial execution date of the dag and not the day the task is run.
    import_answers_to_bigquery = GCSToBigQueryOperator(
        task_id="import_answers_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=["QPI_exports/qpi_answers_{{ ds_nodash }}/*.jsonl"],
        destination_project_dataset_table="{{ bigquery_tmp_dataset }}.{{ ds_nodash }}_qpi_answers_v4",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=False,
        schema_fields=QPI_ANSWERS_SCHEMA,
    )

    append_to_raw = BigQueryInsertJobOperator(
        task_id="add_tmp_table_to_raw",
        configuration={
            "query": {
                "query": "SELECT * FROM {{ bigquery_tmp_dataset }}.{{ ds_nodash }}_qpi_answers_v4",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": "{{ bigquery_raw_dataset }}",
                    "tableId": "qpi_answers_v4",
                },
                "writeDisposition": "WRITE_APPEND",
            }
        },
        trigger_rule="none_failed_or_skipped",
        dag=dag,
    )

    end_raw = DummyOperator(task_id="end_raw")

    end = DummyOperator(task_id="end")

    (
        start
        >> checking_folder_QPI
        >> file
        >> import_historical_answers_to_bigquery
        >> import_answers_to_bigquery
        >> append_to_raw
        >> end_raw
    )
    (checking_folder_QPI >> empty >> end)
