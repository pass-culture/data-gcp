import datetime

from common import macros
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "start_date": datetime.datetime(2020, 12, 20),
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=60),
    "project_id": GCP_PROJECT_ID,
}
dag_id = "dbt_weekly"
dag = DAG(
    dag_id,
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=60),
    catchup=False,
    description="run weekly aggregated models",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[dag_id]),
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
    tags=[DAG_TAGS.DBT.value, DAG_TAGS.DE.value],
)

start = DummyOperator(task_id="start", dag=dag)

wait_for_dbt_daily = delayed_waiting_operator(dag=dag, external_dag_id="dbt_run_dag")

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


start >> wait_for_dbt_daily >> weekly >> end
