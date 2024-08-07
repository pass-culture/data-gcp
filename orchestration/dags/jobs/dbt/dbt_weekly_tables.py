import datetime
import json

from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Param
from common.alerts import task_fail_slack_alert
from common.utils import get_airflow_schedule, waiting_operator

from common import macros
from common.config import (
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    ENV_SHORT_NAME,
    PATH_TO_DBT_TARGET,
)


default_args = {
    "start_date": datetime.datetime(2020, 12, 20),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "dbt_weekly",
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=60),
    catchup=False,
    description="run weekly aggregated models",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    user_defined_macros=macros.default,
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
        "tag": Param(
            default="weekly",
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

weekly = BashOperator(
    task_id="run_weekly",
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


end = DummyOperator(task_id="end", dag=dag, trigger_rule="none_failed")

shunt = DummyOperator(task_id="skip_tasks", dag=dag)


def conditionally_create_tasks(**context):
    execution_date = context["ds"]
    execution_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d")

    if execution_date.weekday() == 0:  # Check if the day is Monday
        return ["wait_for_dbt_run_dag_end"]
    else:
        return ["skip_tasks"]


branching = BranchPythonOperator(
    task_id="branching",
    python_callable=conditionally_create_tasks,
    provide_context=True,
    dag=dag,
)

start >> branching >> wait_for_dbt_daily >> weekly >> end
branching >> shunt >> end
