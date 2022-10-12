import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from common.config import DAG_FOLDER
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
    "import_appsflyer_v1",
    default_args=default_dag_args,
    description="Import Appsflyer tables",
    schedule_interval="00 01 * * *",
    catchup=True,
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

import_data_to_bigquery = SimpleHttpOperator(
    task_id="import_appsflyer_data_to_bigquery",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"appsflyer_import_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_appsflyer_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)

(service_account_token >> import_data_to_bigquery)
