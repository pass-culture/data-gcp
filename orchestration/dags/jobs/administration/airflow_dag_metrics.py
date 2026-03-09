import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.callback import on_failure_base_callback
from common.config import DAG_TAGS, ENV_SHORT_NAME, GCP_PROJECT_ID
from common.hooks.airflow_metrics import collect_airflow_metrics
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "airflow_dag_metrics"

default_dag_args = {
    "start_date": datetime.datetime(2026, 3, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_base_callback,
}

schedule = SCHEDULE_DICT.get(DAG_NAME, {})
if isinstance(schedule, dict):
    schedule = schedule.get(ENV_SHORT_NAME)


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Daily snapshot of Airflow DAG metrics to BigQuery",
    schedule_interval=get_airflow_schedule(schedule),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
    tags=[DAG_TAGS.DE.value],
) as dag:
    collect_metrics = PythonOperator(
        task_id="collect_airflow_metrics",
        python_callable=collect_airflow_metrics,
        op_kwargs={"snapshot_date": "{{ ds }}"},
    )
