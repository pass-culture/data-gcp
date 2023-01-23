import airflow
from airflow import DAG
from common.operators.gce import CleanGCEOperator
from datetime import timedelta

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "airflow_cleaning",
    default_args=default_args,
    description="Automatic cleaning of VMs",
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=1),
)

t1 = CleanGCEOperator(dag=dag, task_id="clean_gce_operator", ttl_min=60 * 24)
