import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)

from common import macros
from common.config import GCP_PROJECT_ID, DAG_FOLDER

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    "dbt_jobs",
    default_args=default_dag_args,
    description="dbt test dag",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
) as dag:

    start = DummyOperator(task_id="start")

    dbt_run_op = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd /opt/airflow/dags/data_gcp_dbt && dbt deps && dbt run --target dev --profiles-dir /opt/airflow/dags/data_gcp_dbt",
    )

start >> dbt_run_op
