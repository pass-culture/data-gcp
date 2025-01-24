from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DATA_GCS_BUCKET_NAME,
    DBT_SCRIPTS_PATH,
    ELEMENTARY_PYTHON_PATH,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
    SLACK_TOKEN_ELEMENTARY,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime, timedelta

SLACK_CHANNEL = "alertes-data-quality"

default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}
dag_id = "dbt_artifacts"
dag = DAG(
    dag_id,
    default_args=default_args,
    catchup=False,
    description="Compute data quality metrics with package elementary and send Slack notifications reports",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[dag_id]),
    user_defined_macros=macros.default,
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
        "GLOBAL_CLI_FLAGS": Param(
            default="",
            type="string",
        ),
    },
    tags=["DBT"],
)

start = DummyOperator(task_id="start", dag=dag)

wait_dbt_run = delayed_waiting_operator(dag=dag, external_dag_id="dbt_run_dag")

compute_metrics_elementary = BashOperator(
    task_id="compute_metrics_elementary",
    bash_command="dbt run --no-write-json --target {{ params.target }} --select package:elementary "
    + f"--target-path {PATH_TO_DBT_TARGET} --profiles-dir {PATH_TO_DBT_PROJECT} "
    + """--vars "{{ "{" }}'ENV_SHORT_NAME':'{{ params.target }}'{{ "}" }}" """,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"bash {DBT_SCRIPTS_PATH}" + "dbt_test.sh ",
    env={
        "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
        "target": "{{ params.target }}",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "EXCLUSION": "audit",
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

send_elementary_report = BashOperator(
    task_id="send_elementary_report",
    bash_command=f"bash {DBT_SCRIPTS_PATH}" + "elementary_send_report.sh ",
    env={
        "PATH_TO_DBT_PROJECT": PATH_TO_DBT_PROJECT,
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "DATA_BUCKET_NAME": DATA_GCS_BUCKET_NAME,
        "REPORT_FILE_PATH": "elementary_reports/{{ execution_date.year }}/elementary_report_{{ execution_date.strftime('%Y%m%d') }}.html",
        "SLACK_TOKEN": SLACK_TOKEN_ELEMENTARY,
        "CHANNEL_NAME": SLACK_CHANNEL,
        "ELEMENTARY_PYTHON_PATH": ELEMENTARY_PYTHON_PATH,
        "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

compile = BashOperator(
    task_id="compilation",
    bash_command=f"bash {DBT_SCRIPTS_PATH}" + "dbt_compile.sh ",
    env={
        "target": "{{ params.target }}",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

(
    start
    >> wait_dbt_run
    >> dbt_test
    >> compute_metrics_elementary
    >> send_elementary_report
    >> compile
)
