from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.slack_alert import task_fail_slack_alert
from dependencies.user_locations_schema import USER_LOCATIONS_SCHEMA
from dependencies.config import (
    GCP_PROJECT,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)


ADDRESSES_FUNCTION_NAME = "addresses_import_" + ENV_SHORT_NAME
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
        + ADDRESSES_FUNCTION_NAME
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


def branch_func(ti, **kwargs):
    xcom_value = ti.xcom_pull(task_ids=["addresses_to_gcs"])
    if "No new users !" not in xcom_value:
        return "import_addresses_to_bigquery"
    else:
        return "end"


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
        http_conn_id="http_gcp_cloud_function",
        endpoint=ADDRESSES_FUNCTION_NAME,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
        },
        log_response=True,
        xcom_push=True,
    )

    branch_op = BranchPythonOperator(
        task_id="checking_if_new_users",
        python_callable=branch_func,
        provide_context=True,
        do_xcom_push=False,
        dag=dag,
    )

    # the tomorrow_ds_nodash enables catchup :
    # it fetches the file corresponding to the initial execution date of the dag and not the day the task is run.
    import_addresses_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="import_addresses_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "addresses_exports/user_locations_{{ tomorrow_ds_nodash }}.csv"
        ],
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.{USER_LOCATIONS_TABLE}",
        write_disposition="WRITE_APPEND",
        source_format="CSV",
        autodetect=False,
        schema_fields=USER_LOCATIONS_SCHEMA,
        skip_leading_rows=1,
        field_delimiter="|",
    )

    to_clean = BigQueryOperator(
        task_id="copy_to_clean",
        sql=f"SELECT * FROM {BIGQUERY_RAW_DATASET}.{USER_LOCATIONS_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{USER_LOCATIONS_TABLE}",
        dag=dag,
    )

    to_analytics = BigQueryOperator(
        task_id="copy_to_analytics",
        sql=f"SELECT * FROM {BIGQUERY_CLEAN_DATASET}.{USER_LOCATIONS_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{USER_LOCATIONS_TABLE}",
        dag=dag,
    )

    end = DummyOperator(task_id="end", trigger_rule="one_success")

    start >> getting_service_account_token >> addresses_to_gcs >> branch_op
    branch_op >> import_addresses_to_bigquery >> to_clean >> to_analytics >> end
    branch_op >> end
