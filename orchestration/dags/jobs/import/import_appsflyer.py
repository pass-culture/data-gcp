import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from dependencies.appsflyer.import_appsflyer import analytics_tables
from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task
from common.utils import depends_loop, get_airflow_schedule
from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID
from common.config import DAG_FOLDER
import json
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER

from common.utils import getting_service_account_token

from common.alerts import task_fail_slack_alert

from common import macros


default_dag_args = {
    "start_date": datetime.datetime(2022, 1, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_appsflyer",
    default_args=default_dag_args,
    description="Import Appsflyer tables",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

service_account_token = PythonOperator(
    task_id="getting_appsflyer_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": f"appsflyer_import_{ENV_SHORT_NAME}"},
    dag=dag,
)

table_defs = {"activity_report": 28, "daily_report": 28, "in_app_event_report": 28}


activity_report_op = SimpleHttpOperator(
    task_id=f"import_activity_report_data_to_bigquery",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"appsflyer_import_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_appsflyer_service_account_token', key='return_value')}}",
    },
    log_response=True,
    data=json.dumps({"table_name": "activity_report", "n_days": 56}),
    dag=dag,
)


daily_report_op = SimpleHttpOperator(
    task_id=f"import_daily_report_data_to_bigquery",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"appsflyer_import_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_appsflyer_service_account_token', key='return_value')}}",
    },
    log_response=True,
    data=json.dumps({"table_name": "daily_report", "n_days": 56}),
    dag=dag,
)


in_app_event_report_op = SimpleHttpOperator(
    task_id=f"import_in_app_event_report_data_to_bigquery",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"appsflyer_import_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_appsflyer_service_account_token', key='return_value')}}",
    },
    log_response=True,
    data=json.dumps({"table_name": "in_app_event_report", "n_days": 28}),
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

(
    service_account_token
    >> daily_report_op
    >> activity_report_op
    >> in_app_event_report_op
    >> start
)
table_jobs >> end
