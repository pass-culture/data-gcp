import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from dependencies.appsflyer.import_appsflyer import analytics_tables
from common.alerts import task_fail_slack_alert
from common.operator import bigquery_job_task
from common.utils import depends_loop
from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT
from common.config import DAG_FOLDER
import json
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT,
    DAG_FOLDER,
)

from common.utils import getting_service_account_token

from common.alerts import task_fail_slack_alert

from common import macros


default_dag_args = {
    "start_date": datetime.datetime(2022, 1, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_appsflyer",
    default_args=default_dag_args,
    description="Import Appsflyer tables",
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

service_account_token = PythonOperator(
    task_id="getting_appsflyer_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={
        "function_name": f"appsflyer_import_{ENV_SHORT_NAME}",
    },
    dag=dag,
)

table_defs = {"activity_report": 56, "daily_report": 56, "in_app_event_report": 28}

http_ops = []

for table_name, n_days in table_defs.items():
    op = SimpleHttpOperator(
        task_id=f"import_{table_name}_data_to_bigquery",
        method="POST",
        http_conn_id="http_gcp_cloud_function",
        endpoint=f"appsflyer_import_{ENV_SHORT_NAME}",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_appsflyer_service_account_token', key='return_value')}}",
        },
        log_response=True,
        data=json.dumps({"table_name": table_name, "n_days": n_days}),
        dag=dag,
    )
    http_ops.append(op)

start = DummyOperator(task_id="start", dag=dag)

table_jobs = {}
for table, job_params in analytics_tables.items():
    task = bigquery_job_task(dag, table, job_params)
    table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
    }

table_jobs = depends_loop(table_jobs, start)
end = DummyOperator(task_id="end", dag=dag)

service_account_token >> http_ops >> start
table_jobs >> end
