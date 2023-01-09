import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from dependencies.adage.import_adage import analytics_tables

from google.auth.transport.requests import Request
from google.oauth2 import id_token
from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task
from common.utils import depends_loop
from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER

from common.config import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
)

FUNCTION_NAME = f"adage_import_{ENV_SHORT_NAME}"
SIREN_FILENAME = "adage_data.csv"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


def getting_service_account_token():
    function_url = (
        f"https://europe-west1-{GCP_PROJECT_ID}.cloudfunctions.net/{FUNCTION_NAME}"
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


dag = DAG(
    "import_adage_v1",
    default_args=default_dag_args,
    description="Import Adage from API",
    on_failure_callback=None,
    # Cannot Schedule before 5AM UTC+2 as data from API is not available.
    schedule_interval="0 1 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

getting_service_account_token = PythonOperator(
    task_id="getting_service_account_token",
    python_callable=getting_service_account_token,
    dag=dag,
)

adage_to_bq = SimpleHttpOperator(
    task_id="adage_to_bq",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=FUNCTION_NAME,
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
    },
    dag=dag,
)


start = DummyOperator(task_id="start", dag=dag)

table_jobs = {}
for table, job_params in analytics_tables.items():
    task = bigquery_job_task(dag, table, job_params)
    table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
    }

table_jobs = depends_loop(table_jobs, start, dag=dag)
end = DummyOperator(task_id="end", dag=dag)

getting_service_account_token >> adage_to_bq >> start
table_jobs >> end
