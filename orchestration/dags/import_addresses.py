from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.slack_alert import task_fail_slack_alert
from dependencies.user_locations_schema import USER_LOCATIONS_SCHEMA
from dependencies.config import (
    GCP_PROJECT,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)


TYPEFORM_FUNCTION_NAME = "addresses_import_" + ENV_SHORT_NAME
USER_LOCATIONS_TABLE = "user_locations"

default_args = {
    "start_date": datetime(2021, 3, 30),
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


with DAG(
    "import_addresses_v1",
    default_args=default_args,
    description="Importing new data from addresses api every day.",
    schedule_interval="30 2 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    getting_service_account_token = PythonOperator(
        task_id="getting_service_account_token",
        python_callable=getting_service_account_token,
    )

    addresses_to_gcs = SimpleHttpOperator(
        task_id="addresses_to_gcs",
        method="POST",
        http_conn_id="http_addresses_function",
        endpoint=TYPEFORM_FUNCTION_NAME,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
        },
        log_response=True,
    )

    # the tomorrow_ds_nodash enables catchup :
    # it fetches the file corresponding to the initial execution date of the dag and not the day the task is run.
    import_addresses_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="import_addresses_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "addresses_exports/user_locations_{{ tomorrow_ds_nodash }}.csv"
        ],
        destination_project_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{USER_LOCATIONS_TABLE}",
        write_disposition="WRITE_APPEND",
        source_format="CSV",
        autodetect=False,
        schema_fields=USER_LOCATIONS_SCHEMA,
    )

    end = DummyOperator(task_id="end")

    start >> getting_service_account_token >> addresses_to_gcs >> import_addresses_to_bigquery >> end
