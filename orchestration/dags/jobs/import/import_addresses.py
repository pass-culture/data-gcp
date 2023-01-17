from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.python import BranchPythonOperator, PythonOperator

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from common.config import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
)
from common.alerts import task_fail_slack_alert
from common.utils import getting_service_account_token
from dependencies.import_addresses import USER_LOCATIONS_SCHEMA


FUNCTION_NAME = f"addresses_import_{ENV_SHORT_NAME}"
USER_LOCATIONS_TABLE = "user_locations"

default_args = {
    "start_date": datetime(2021, 3, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def branch_function(ti, **kwargs):
    xcom_value = ti.xcom_pull(task_ids=["addresses_to_gcs"])
    if "No new users !" not in xcom_value:
        return "import_addresses_to_bigquery"
    else:
        return "end"


with DAG(
    "import_addresses_v1",
    default_args=default_args,
    description="Importing new data from addresses api every day.",
    # every 10 minutes if prod once a day otherwise
    schedule_interval="*/10 * * * *" if ENV_SHORT_NAME == "prod" else "30 2 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    getting_service_account_token = PythonOperator(
        task_id="getting_service_account_token",
        python_callable=getting_service_account_token,
        op_kwargs={
            "function_name": FUNCTION_NAME,
        },
    )

    addresses_to_gcs = SimpleHttpOperator(
        task_id="addresses_to_gcs",
        method="POST",
        http_conn_id="http_gcp_cloud_function",
        endpoint=FUNCTION_NAME,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
        },
        log_response=True,
        do_xcom_push=True,
    )

    branch_op = BranchPythonOperator(
        task_id="checking_if_new_users",
        python_callable=branch_function,
        provide_context=True,
        do_xcom_push=False,
        dag=dag,
    )

    import_addresses_to_bigquery = GCSToBigQueryOperator(
        task_id="import_addresses_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "{{task_instance.xcom_pull(task_ids='addresses_to_gcs', key='return_value')}}"
        ],
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.{USER_LOCATIONS_TABLE}",
        write_disposition="WRITE_APPEND",
        source_format="CSV",
        autodetect=False,
        schema_fields=USER_LOCATIONS_SCHEMA,
        skip_leading_rows=1,
        field_delimiter="|",
    )

    to_clean = BigQueryExecuteQueryOperator(
        task_id="copy_to_clean",
        sql=f"""
            SELECT * EXCEPT(id, irisCode, centroid, shape), iris_france.id AS iris_id
            FROM {BIGQUERY_RAW_DATASET}.{USER_LOCATIONS_TABLE}
            LEFT JOIN `{BIGQUERY_CLEAN_DATASET}.iris_france` iris_france
            ON 1 = 1
            WHERE ST_CONTAINS(shape, ST_GEOGPOINT(longitude, latitude))
        """,
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{USER_LOCATIONS_TABLE}",
        dag=dag,
    )

    to_analytics = BigQueryExecuteQueryOperator(
        task_id="copy_to_analytics",
        sql=f"""
            SELECT * EXCEPT (user_address) 
            FROM {BIGQUERY_CLEAN_DATASET}.{USER_LOCATIONS_TABLE}
            QUALIFY ROW_NUMBER() over (PARTITION BY user_id ORDER BY date_updated DESC) = 1
            
        """,
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{USER_LOCATIONS_TABLE}",
        dag=dag,
    )

    end = DummyOperator(task_id="end", trigger_rule="one_success")

    start >> getting_service_account_token >> addresses_to_gcs >> branch_op
    branch_op >> import_addresses_to_bigquery >> to_clean >> to_analytics >> end
    branch_op >> end
