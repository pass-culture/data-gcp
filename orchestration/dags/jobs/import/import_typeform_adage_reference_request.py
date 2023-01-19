import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from common.alerts import task_fail_slack_alert


from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.utils import getting_service_account_token, get_airflow_schedule

FUNCTION_NAME = f"typeform_adage_reference_request_{ENV_SHORT_NAME}"


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


dag = DAG(
    "import_typeform_adage_reference_request",
    default_args=default_dag_args,
    description="Import Typeform Adage Reference Request from API",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)

getting_service_account_token = PythonOperator(
    task_id="getting_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": f"{FUNCTION_NAME}"},
    dag=dag,
)

typeform_adage_reference_request_to_bq = SimpleHttpOperator(
    task_id="typeform_adage_reference_request_to_bq",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=FUNCTION_NAME,
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
    },
    dag=dag,
)


create_analytics_table = BigQueryExecuteQueryOperator(
    task_id="create_enriched_app_downloads_stats",
    sql=f"""
    SELECT 
        *
    FROM `{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.typeform_adage_reference_request_sheet` 
    """,
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.typeform_adage_reference_request",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)


end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> getting_service_account_token
    >> typeform_adage_reference_request_to_bq
    >> create_analytics_table
    >> end
)
