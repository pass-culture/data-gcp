import json
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.bigquery_client import BigQueryClient
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.qpi_answers_schema import QPI_ANSWERS_SCHEMA
from dependencies.config import (
    GCP_PROJECT,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)
from dependencies.data_analytics.enriched_data.enriched_qpi_answers_v2 import (
    enrich_answers,
    format_answers,
)

TYPEFORM_FUNCTION_NAME = "qpi_import_" + ENV_SHORT_NAME
QPI_ANSWERS_TABLE = "qpi_answers_v3"

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
        http_conn_id="http_gcp_cloud_function",
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

    # the tomorrow_ds_nodash enables catchup :
    # it fetches the file corresponding to the initial execution date of the dag and not the day the task is run.
    import_answers_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="import_answers_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=["QPI_exports/qpi_answers_{{ tomorrow_ds_nodash }}.jsonl"],
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=False,
        schema_fields=QPI_ANSWERS_SCHEMA,
    )

    add_answers_to_raw = BigQueryOperator(
        task_id="add_answers_to_raw",
        sql=f"""
            select *
            FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}` 
        """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_RAW_DATASET}.{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_APPEND",
    )

    add_answers_to_clean = BigQueryOperator(
        task_id="add_answers_to_clean",
        sql=f"""
            select (CASE raw_answers.user_id WHEN null THEN users.user_id else raw_answers.user_id END) as user_id,
            landed_at, submitted_at, form_id, platform, answers,
            CAST(NULL AS STRING) AS catch_up_user_id
            FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}` raw_answers
            LEFT JOIN `{GCP_PROJECT}.{'clean_stg' if ENV_SHORT_NAME == 'dev' else BIGQUERY_CLEAN_DATASET}.applicative_database_user` users
            ON raw_answers.culturalsurvey_id = users.user_cultural_survey_id
        """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_CLEAN_DATASET}.{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_APPEND",
    )

    delete_temp_answer_table = BigQueryTableDeleteOperator(
        task_id="delete_temp_answer_table",
        deletion_dataset_table=f"{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}",
        ignore_if_missing=True,
        dag=dag,
    )

    enrich_qpi_answers = BigQueryOperator(
        task_id="enrich_qpi_answers",
        sql=enrich_answers(
            gcp_project=GCP_PROJECT,
            bigquery_clean_dataset=BIGQUERY_CLEAN_DATASET,
        ),
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_ANALYTICS_DATASET}.enriched_{QPI_ANSWERS_TABLE}_temp",
        write_disposition="WRITE_TRUNCATE",
    )

    format_qpi_answers = PythonOperator(
        task_id="format_qpi_answers",
        python_callable=format_answers,
        op_kwargs={
            "gcp_project": GCP_PROJECT,
            "bigquery_analytics_dataset": BIGQUERY_ANALYTICS_DATASET,
            "enriched_qpi_answer_table": f"enriched_{QPI_ANSWERS_TABLE}",
        },
        dag=dag,
    )

    end = DummyOperator(task_id="end")

    start >> getting_last_token >> getting_service_account_token >> typeform_to_gcs
    (
        typeform_to_gcs
        >> import_answers_to_bigquery
        >> add_answers_to_raw
        >> add_answers_to_clean
        >> delete_temp_answer_table
        >> enrich_qpi_answers
        >> format_qpi_answers
        >> end
    )
