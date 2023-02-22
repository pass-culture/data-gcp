import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
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
    log_response=True,
    dag=dag,
)


create_analytics_table = BigQueryInsertJobOperator(
    dag=dag,
    task_id="typeform_adage_reference_request_to_analytics",
    configuration={
        "query": {
            "query": f"SELECT * FROM `{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.typeform_adage_reference_request_sheet`",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BIGQUERY_ANALYTICS_DATASET,
                "tableId": "typeform_adage_reference_request",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> getting_service_account_token
    >> typeform_adage_reference_request_to_bq
    >> create_analytics_table
    >> end
)
