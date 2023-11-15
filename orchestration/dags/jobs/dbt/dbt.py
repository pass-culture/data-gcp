import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)

from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    "compile_dbt",
    default_args=default_dag_args,
    description="Compile dbt test",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
) as dag:

    start = DummyOperator(task_id="start")
