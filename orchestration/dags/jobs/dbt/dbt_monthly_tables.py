import datetime

from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from jobs.crons import schedule_dict

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

default_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}
dag_id = "dbt_monthly"
dag = DAG(
    dag_id,
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=60),
    catchup=False,
    description="run monthly aggregated models",
    schedule_interval=get_airflow_schedule(schedule_dict[dag_id]),
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

end = DummyOperator(task_id="end", dag=dag, trigger_rule="none_failed")

wait_for_dbt_daily = delayed_waiting_operator(dag=dag, external_dag_id="dbt_run_dag")

shunt = DummyOperator(task_id="skip_tasks", dag=dag)

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


def conditionally_create_tasks(**context):
    execution_date = context["ds"]
    execution_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d")

    if execution_date.day == 1:  # Check if the day is the first of the month
        return ["wait_for_dbt_run_dag_end"]
    else:
        return ["skip_tasks"]


branching = BranchPythonOperator(
    task_id="branching",
    python_callable=conditionally_create_tasks,
    provide_context=True,
    dag=dag,
)

start >> branching >> wait_for_dbt_daily >> monthly >> end
branching >> shunt >> end
