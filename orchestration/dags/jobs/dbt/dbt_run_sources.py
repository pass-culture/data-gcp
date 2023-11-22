import datetime
from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)

from common import macros
from common.config import GCP_PROJECT_ID, DAG_FOLDER, PATH_TO_DBT_PROJECT


# source {PATH_TO_DBT_VENV} &&
# PATH_TO_DBT_VENV = "3.10.4/envs/dbt-venv"
# PATH_TO_DBT_PROJECT ="dags/data_gcp_dbt"


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}
# dag_config = {
#     "PATH_TO_DBT_VENV": None,
#     "PATH_TO_DBT_PROJECT": "orchestration/dags/data_gcp_dbt"

# }

with DAG(
    "dbt_source_jobs",
    default_args=default_dag_args,
    description="dbt test dag",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "target": Param(
            default="dev",
            type="string",
        ),
        "folder": Param(
            default="clean",
            type="string",
        ),
        "children": Param(
            default="",
            type="string",
        ),
    },
) as dag:

    start = DummyOperator(task_id="start")

    dbt_run_op = BashOperator(
        task_id="run_selective_dbt",
        bash_command="dbt run --target {{ params.target }} --select models.{{ params.folder }}.*{{ params.children }}",
        cwd=PATH_TO_DBT_PROJECT,
    )

start >> dbt_run_op
