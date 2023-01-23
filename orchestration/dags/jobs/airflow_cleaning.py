import airflow
from airflow import DAG
from common.operators.gce import CleanGCEOperator
from datetime import timedelta
from common.config import ENV_SHORT_NAME
from common.utils import get_airflow_schedule

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


clean_delay = {"prod": 60 * 24, "dev": 60 * 8, "stg": 60 * 8}


dag = DAG(
    "airflow_cleaning",
    default_args=default_args,
    description="Automatic cleaning of VMs",
    schedule_interval=get_airflow_schedule("0 * * * *"),
    dagrun_timeout=timedelta(hours=1),
)

t1 = CleanGCEOperator(
    dag=dag,
    task_id="clean_gce_operator",
    timeout_in_minutes=clean_delay[ENV_SHORT_NAME],
)
