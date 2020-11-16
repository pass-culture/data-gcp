"""A test dag"""
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "airflow_deploy_test",
    default_args=default_args,
    description="test deploy dag",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20),
)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = DummyOperator(task_id="start", dag=dag)
t2 = DummyOperator(task_id="end", dag=dag)

t1 >> t2