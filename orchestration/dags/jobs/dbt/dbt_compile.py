from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from common.config import PATH_TO_DBT_PROJECT


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 23),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dbt_dag",
    default_args=default_args,
    description="A dbt wrapper for airflow",
    schedule_interval=timedelta(days=1),
)
compile_task = BashOperator(
            task_id="c",
            bash_command=f"""
            dbt compile --target dev
            """,
            dag=dag,
            cwd=PATH_TO_DBT_PROJECT,
        )
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
    "dbt_compile",
    default_args=default_dag_args,
    description="compile dbt project",
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

    dbt_compile_op = BashOperator(
        task_id="run_compile_dbt",
        bash_command="dbt compile --target {{ params.target }}",
        cwd=PATH_TO_DBT_PROJECT,
    )

start >> dbt_compile_op