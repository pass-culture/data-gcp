import os
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from common.utils import (
    getting_service_account_token
)

from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT,
    DAG_FOLDER,
    ENV_SHORT_NAME
)

from common import macros


default_dag_args = {
    "start_date": datetime(2022, 1, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}


with DAG(
    "metabase_archiving",
    default_args=default_dag_args,
    description="Launch archiving script for Metabase cards",
    schedule_interval="0 0 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    service_account_token = PythonOperator(
        task_id="getting_metabase_archiving_service_account_token",
        python_callable=getting_service_account_token,
        op_kwargs={
            "function_name": f"metabase_archiving_{ENV_SHORT_NAME}",
        },
        dag=dag,
    )

    archive_metabase_cards_op = SimpleHttpOperator(
        task_id=f"archive_metabase_cards",
        method="POST",
        http_conn_id="http_gcp_cloud_function",
        endpoint=f"metabase_archiving_{ENV_SHORT_NAME}",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_metabase_archiving_service_account_token', key='return_value')}}",
        },
        log_response=True,
        # data=json.dumps({}),
        dag=dag,
    )
    
    end = DummyOperator(task_id="end", dag=dag)

    start >> service_account_token >> archive_metabase_cards_op >> end
