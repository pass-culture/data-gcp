import datetime
import json
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    DAG_FOLDER,
)
from common.operators.biquery import bigquery_job_task
from common.utils import (
    getting_service_account_token,
    depends_loop,
    get_airflow_schedule,
)

from common.alerts import task_fail_slack_alert

from common import macros

from dependencies.sendinblue.import_sendinblue import analytics_tables

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_sendinblue",
    default_args=default_dag_args,
    description="Import sendinblue tables",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

service_account_token = PythonOperator(
    task_id="getting_sendinblue_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": f"sendinblue_import_{ENV_SHORT_NAME}"},
    dag=dag,
)

import_transactional_data_to_raw = SimpleHttpOperator(
    task_id="import_transactional_data_to_raw",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"sendinblue_import_{ENV_SHORT_NAME}",
    data=json.dumps({"target": "transactional"}),
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_sendinblue_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)

import_newsletter_data_to_raw = SimpleHttpOperator(
    task_id="import_newsletter_data_to_raw",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"sendinblue_import_{ENV_SHORT_NAME}",
    data=json.dumps({"target": "newsletter"}),
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_sendinblue_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)


end_raw = DummyOperator(task_id="end_raw", dag=dag)

analytics_table_jobs = {}
for name, params in analytics_tables.items():
    task = bigquery_job_task(dag=dag, table=name, job_params=params)
    analytics_table_jobs[name] = {
        "operator": task,
        "depends": params.get("depends", []),
        "dag_depends": params.get("dag_depends", []),
    }

    # import_tables_to_analytics_tasks.append(task)

analytics_table_tasks = depends_loop(analytics_table_jobs, end_raw, dag=dag)

(
    service_account_token
    >> import_newsletter_data_to_raw
    >> end_raw
    >> analytics_table_tasks
)

service_account_token >> import_transactional_data_to_raw >> end_raw
