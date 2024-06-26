import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime, timedelta
from airflow.models import Param
from common.utils import get_airflow_schedule, waiting_operator

from common.config import (
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    ENV_SHORT_NAME,
    PATH_TO_DBT_TARGET,
)


default_args = {
    "start_date": datetime(2020, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "dbt_monthly",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    description="run monthly aggregated models",
    schedule_interval=get_airflow_schedule(
        "0 1 1 * *"
    ),  # run every 1st day of the month at 1 AM
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
        "tag": Param(
            default="monthly",
            type="string",
        ),
        "GLOBAL_CLI_FLAGS": Param(
            default="--no-write-json",
            type="string",
        ),
        "full_refresh": Param(
            default=False,
            type="boolean",
        ),
        "exclude": Param(
            default="",
            type="string",
        ),
    },
)

start = DummyOperator(task_id="start", dag=dag)

wait_for_dbt_daily = waiting_operator(dag=dag, dag_id="dbt_run_dag")

monthly = BashOperator(
    task_id="run_monthly",
    dag=dag,
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run_tag.sh ",
    env={
        "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
        "target": "{{ params.target }}",
        "tag": "{{ params.tag }}",
        "full_ref_str": " --full-refresh" if not "{{ params.full_refresh }}" else "",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        "EXCLUSION": "{{ params.exclude }}",
    },
    cwd=PATH_TO_DBT_PROJECT,
    append_env=True,
)


end = DummyOperator(task_id="end", dag=dag, trigger_rule="all_success")


start >> wait_for_dbt_daily >> monthly >> end
