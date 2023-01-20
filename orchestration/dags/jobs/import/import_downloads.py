import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from common.operators.biquery import bigquery_job_task

from common.config import DAG_FOLDER

from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    DAG_FOLDER,
)

from common.utils import getting_service_account_token, get_airflow_schedule

from common.alerts import task_fail_slack_alert

from common import macros

from dependencies.downloads.import_downloads import ANALYTICS_TABLES


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_downloads",
    default_args=default_dag_args,
    description="Import downloads tables",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

getting_downloads_service_account_token = PythonOperator(
    task_id="getting_downloads_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": f"downloads_{ENV_SHORT_NAME}"},
    dag=dag,
)

import_downloads_data_to_bigquery = SimpleHttpOperator(
    task_id="import_downloads_data_to_bigquery",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"downloads_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_downloads_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)
# only downloads included here
analytics_tasks = []
for table, params in ANALYTICS_TABLES.items():
    task = bigquery_job_task(table=table, dag=dag, job_params=params)
    analytics_tasks.append(task)

end = DummyOperator(task_id="end", dag=dag)
(
    getting_downloads_service_account_token
    >> import_downloads_data_to_bigquery
    >> analytics_tasks
    >> end
)
