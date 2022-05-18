import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import (
    GoogleCloudStorageToBigQueryOperator,
)

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from common.alerts import task_fail_slack_alert

from dependencies.config import (
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)
from dependencies.dms_import import parse_api_result

DMS_FUNCTION_NAME = "dms_" + ENV_SHORT_NAME


def getting_service_account_token():
    function_url = (
        "https://europe-west1-"
        + GCP_PROJECT
        + ".cloudfunctions.net/"
        + DMS_FUNCTION_NAME
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


default_args = {
    "start_date": datetime(2021, 8, 29),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "import_dms_subscriptions",
    default_args=default_args,
    description="Import DMS subscriptions",
    schedule_interval="0 1 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    getting_service_account_token = PythonOperator(
        task_id="getting_service_account_token",
        python_callable=getting_service_account_token,
    )

    dms_to_gcs = SimpleHttpOperator(
        task_id="dms_to_gcs",
        method="POST",
        http_conn_id="http_gcp_cloud_function",
        endpoint=DMS_FUNCTION_NAME,
        data=json.dumps({"updated_since": "{{ prev_start_date_success.isoformat() }}"}),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
        },
        log_response=True,
        xcom_push=True,
    )

    parse_api_result_jeunes = PythonOperator(
        task_id="parse_api_result_jeunes",
        python_callable=parse_api_result,
        op_args=[
            "{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}",
            "jeunes",
        ],
        dag=dag,
    )

    parse_api_result_pro = PythonOperator(
        task_id="parse_api_result_pro",
        python_callable=parse_api_result,
        op_args=[
            "{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}",
            "pro",
        ],
        dag=dag,
    )

    import_dms_jeunes_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="import_dms_jeunes_to_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "dms_export/dms_jeunes_{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}.parquet"
        ],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.dms_jeunes",
        schema_fields=[
            {"name": "procedure_id", "type": "STRING"},
            {"name": "application_id", "type": "STRING"},
            {"name": "application_number", "type": "STRING"},
            {"name": "application_archived", "type": "STRING"},
            {"name": "application_status", "type": "STRING"},
            {"name": "last_update_at", "type": "TIMESTAMP"},
            {"name": "application_submitted_at", "type": "TIMESTAMP"},
            {"name": "passed_in_instruction_at", "type": "TIMESTAMP"},
            {"name": "processed_at", "type": "TIMESTAMP"},
            {"name": "application_motivation", "type": "STRING"},
            {"name": "instructors", "type": "STRING"},
            {"name": "applicant_department", "type": "STRING"},
            {"name": "applicant_postal_code", "type": "STRING"},
        ],
        write_disposition="WRITE_APPEND",
    )

    import_dms_pro_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="import_dms_pro_to_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "dms_export/dms_pro_{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}.parquet"
        ],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.dms_pro",
        write_disposition="WRITE_APPEND",
    )

    def deduplicate_query(target):
        return f"""
        SELECT * except(row_number)
        FROM (
            SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY application_number
                                            ORDER BY last_update_at DESC
                                        ) as row_number
            FROM `{BIGQUERY_CLEAN_DATASET}.dms_{target}`
            )
        WHERE row_number=1
        """

    copy_dms_jeunes_to_analytics = BigQueryOperator(
        task_id="copy_dms_jeunes_to_analytics",
        sql=deduplicate_query("jeunes"),
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.dms_jeunes",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        dag=dag,
    )

    copy_dms_pro_to_analytics = BigQueryOperator(
        task_id="copy_dms_pro_to_analytics",
        sql=deduplicate_query("pro"),
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.dms_pro",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        dag=dag,
    )

    end = DummyOperator(task_id="end")


(
    start
    >> getting_service_account_token
    >> dms_to_gcs
    >> [parse_api_result_jeunes, parse_api_result_pro]
)

parse_api_result_jeunes >> import_dms_jeunes_to_bq >> copy_dms_jeunes_to_analytics
parse_api_result_pro >> import_dms_pro_to_bq >> copy_dms_pro_to_analytics

[copy_dms_jeunes_to_analytics, copy_dms_pro_to_analytics] >> end
