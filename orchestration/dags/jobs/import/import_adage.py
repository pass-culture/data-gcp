import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from dependencies.adage.import_adage import analytics_tables
from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task
from common.operators.sensor import TimeSleepSensor
from common.utils import depends_loop, getting_service_account_token
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


dag = DAG(
    "import_adage_v1",
    start_date=datetime.datetime(2020, 12, 1),
    default_args=default_dag_args,
    description="Import Adage from API",
    on_failure_callback=None,
    schedule_interval="0 1 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

# Cannot Schedule before 5AM UTC+2 as data from API is not available.
# exec = day minus 1
sleep_op = TimeSleepSensor(
    task_id="sleep_task",
    sleep_duration=datetime.timedelta(days=1, minutes=120),  # 2H
    mode="reschedule",
)

sa_token_op = PythonOperator(
    task_id="getting_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={
        "function_name": FUNCTION_NAME,
    },
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
        "dag_depends": job_params.get("dag_depends", []),
    }

table_jobs = depends_loop(table_jobs, start, dag=dag)
end = DummyOperator(task_id="end", dag=dag)

sleep_op >> sa_token_op >> adage_to_bq >> start
table_jobs >> end
