import json
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.bigquery_client import BigQueryClient
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.config import (
    GCP_PROJECT,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
)

TYPEFORM_FUNCTION_NAME = "qpi_import_" + ENV_SHORT_NAME
QPI_ANSWERS_TABLE = "qpi_answers_test"  # TO CHANGE

default_args = {
    "start_date": datetime(2021, 3, 10),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def getting_service_account_token():
    function_url = (
        "https://europe-west1-"
        + GCP_PROJECT
        + ".cloudfunctions.net/"
        + TYPEFORM_FUNCTION_NAME
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


def getting_last_token(project_name, dataset, table):
    bigquery_query = f"SELECT form_id FROM {project_name}.{dataset}.{table} ORDER BY submitted_at DESC LIMIT 1;"
    bigquery_client = BigQueryClient()
    results = bigquery_client.query(bigquery_query)
    return results.values[0][0]


with DAG(
    "import_typeform_v1",
    default_args=default_args,
    description="Importing new data from typeform every day.",
    schedule_interval="0 2 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    getting_last_token = PythonOperator(
        task_id="getting_last_token",
        python_callable=getting_last_token,
        op_kwargs={
            "project_name": GCP_PROJECT,
            "dataset": BIGQUERY_RAW_DATASET,
            "table": QPI_ANSWERS_TABLE,
        },
    )

    getting_service_account_token = PythonOperator(
        task_id="getting_service_account_token",
        python_callable=getting_service_account_token,
    )

    typeform_to_gcs = SimpleHttpOperator(
        task_id="typeform_to_gcs",
        method="POST",
        http_conn_id="http_typeform_function",
        endpoint=TYPEFORM_FUNCTION_NAME,
        data=json.dumps(
            {
                "after": "{{task_instance.xcom_pull(task_ids='getting_last_token', key='return_value')}}"
            }
        ),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
        },
        log_response=True,
    )

    today = date.today().strftime("%Y%m%d")

    import_answers_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="import_answers_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[f"QPI_exports/qpi_answers_{today}.jsonl"],
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
    )

    end = DummyOperator(task_id="end")

    start >> getting_last_token >> getting_service_account_token >> typeform_to_gcs
    typeform_to_gcs >> import_answers_to_bigquery >> end
