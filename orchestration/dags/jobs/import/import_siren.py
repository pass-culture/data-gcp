import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from common.config import DAG_FOLDER
from common.config import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
)
from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task
from dependencies.siren.import_siren import ANALYTICS_TABLES
from common.utils import getting_service_account_token, get_airflow_schedule
from common import macros

FUNCTION_NAME = f"siren_import_{ENV_SHORT_NAME}"
SIREN_FILENAME = "siren_data.csv"
schedule_interval = "0 */6 * * *" if ENV_SHORT_NAME == "prod" else "30 */6 * * *"

default_dag_args = {
    "start_date": datetime.datetime(2021, 8, 25),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_siren_v1",
    default_args=default_dag_args,
    description="Import Siren from INSEE API",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule(schedule_interval),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

getting_service_account_token = PythonOperator(
    task_id="getting_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": f"{FUNCTION_NAME}"},
    dag=dag,
)

siren_to_bq = SimpleHttpOperator(
    task_id="siren_to_bq",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=FUNCTION_NAME,
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
    },
    dag=dag,
)

analytics_tasks = []
for table, params in ANALYTICS_TABLES.items():
    task = bigquery_job_task(dag, table, params)
    analytics_tasks.append(task)

start = DummyOperator(task_id="start", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

(start >> getting_service_account_token >> siren_to_bq >> analytics_tasks >> end)
