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
    "dbt_init_dag",
    default_args=default_args,
    catchup=False,
    description="A dbt wrapper for airflow",
    schedule_interval=None,#get_airflow_schedule("0 3 * * *"),
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
        "GLOBAL_CLI_FLAGS": Param(
            default="",
            type="string",
        ),
        "full_refresh": Param(
            default=False,
            type="boolean",
        ),
    },
)

start = DummyOperator(task_id="start", dag=dag)

deps = BashOperator(
                task_id="dbt_packages_dependencies",
                bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_deps.sh ",
                            env={
                                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                                "target": "{{ params.target }}",
                                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                            },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
                dag=dag,
)

manifest = BashOperator(
                task_id="dbt_manifest",
                bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_manifest.sh ",
                            env={
                                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                                "target": "{{ params.target }}",
                                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                            },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
                dag=dag,
)

run_elementary = BashOperator(
                task_id="dbt_elementary",
                bash_command=f"bash ./scripts/dbt_run_package.sh ",
                            env={
                                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                                "target": "{{ params.target }}",
                                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                                "package_name": "elementary"
                            },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
                dag=dag, 
)

run_re_data = BashOperator(
                task_id="dbt_re_data",
                bash_command=f"bash ./scripts/dbt_run_package.sh ",
                            env={
                                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                                "target": "{{ params.target }}",
                                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                                "package_name": "re_data"
                            },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
                dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> deps >> manifest >> (run_re_data, run_elementary) >> end