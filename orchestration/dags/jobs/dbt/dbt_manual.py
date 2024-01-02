import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from airflow.models import Param
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)
from common.dbt.utils import rebuild_manifest

from common import macros
from common.config import (
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    ENV_SHORT_NAME,
    PATH_TO_DBT_TARGET,
)


default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "dbt_manual_dag",
    default_args=default_args,
    catchup=False,
    description="A dbt wrapper for airflow",
    schedule_interval=None,
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
        "command": Param(
            default="dbt run --models package:re_data",
            type="string",
        ),
    },
)

start = DummyOperator(task_id="start", dag=dag)

dbt_manual_command = BashOperator(
    task_id="dbt_manual_command",
    bash_command="{{ params.command }}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> dbt_manual_command >> end
