import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.slack_alert import task_fail_slack_alert
from dependencies.request_dms import update_dms_applications


default_args = {
    "start_date": datetime(2021, 5, 26),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "import_dms_subscriptions",
    default_args=default_args,
    description="Import DMS subscriptions",
    schedule_interval="@daily",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    fetch_dms = PythonOperator(
        task_id=f"download_dms_subscriptions",
        python_callable=update_dms_applications,
    )

    end = DummyOperator(task_id="end")


start >> fetch_dms >> end
