import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from dependencies.config import ENV_SHORT_NAME, GCP_PROJECT
from dependencies.env_switcher import env_switcher
from dependencies.slack_alert import task_fail_slack_alert

default_dag_args = {
    "start_date": datetime.datetime(2021, 3, 3),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_firebase_data_v1",
    default_args=default_dag_args,
    description="Import firebase data and dispatch it to each env",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 5 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

env_switcher = BranchPythonOperator(
    task_id="env_switcher",
    python_callable=env_switcher,
    dag=dag,
)

copy_firebase_data = DummyOperator(task_id="copy_firebase_data", dag=dag)
clear_firebase_data = DummyOperator(task_id="clear_firebase_data", dag=dag)

dummy_task = DummyOperator(task_id="dummy_task", dag=dag)

copy_to_env = DummyOperator(task_id=f"copy_to_{ENV_SHORT_NAME}", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> env_switcher
env_switcher >> dummy_task >> copy_to_env
env_switcher >> copy_firebase_data >> clear_firebase_data >> copy_to_env >> end
