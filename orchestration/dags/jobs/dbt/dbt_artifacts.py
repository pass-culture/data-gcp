from common import macros
from common.callback import on_failure_base_callback
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
    SLACK_CHANNEL_DATA_QUALITY,
    SLACK_TOKEN_DATA_QUALITY,
)
from common.operators.monitoring import (
    GenerateElementaryReportOperator,
    SendElementaryMonitoringReportOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import datetime, timedelta


def should_run_today():
    if datetime.today().weekday() == 0:  # Monday
        return ["dbt_test_weekly", "dbt_test"]
    return "dbt_test"


default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 1,
    "on_failure_callback": on_failure_base_callback,
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
        "send_slack_report": Param(
            default=True if ENV_SHORT_NAME == "prod" else False,
            type="boolean",
        ),
        "slack_group_alerts_by": Param(
            default="table",
            type="string",
        ),
    },
    tags=[DAG_TAGS.DBT.value, DAG_TAGS.DE.value],
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
    trigger_rule="one_success",
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test.sh ",
    env={
        "target": "{{ params.target }}",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "EXCLUSION": "audit tag:export tag:weekly",
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

check_if_weekly = BranchPythonOperator(
    task_id="check_if_weekly_run", python_callable=should_run_today
)

dbt_test_weekly = BashOperator(
    task_id="dbt_test_weekly",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test.sh ",
    env={
        "target": "{{ params.target }}",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "EXCLUSION": "audit tag:export",
        "SELECT": "tag:weekly",
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
    trigger_rule="all_success",
)


create_elementary_report = GenerateElementaryReportOperator(
    task_id="create_elementary_report",
    report_file_path="elementary_reports/{{ execution_date.year }}/elementary_report_{{ execution_date.strftime('%Y%m%d') }}.html",
    days_back=14,
)

send_elementary_report = SendElementaryMonitoringReportOperator(
    task_id="send_elementary_report",
    slack_channel=SLACK_CHANNEL_DATA_QUALITY,
    slack_token=SLACK_TOKEN_DATA_QUALITY,
    days_back=1,
    slack_group_alerts_by="{{ params.slack_group_alerts_by }}",
    global_suppression_interval=0,
    send_slack_report="{{ params.send_slack_report }}",
)

recompile_dbt_project = BashOperator(
    task_id="recompile_dbt_project",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_compile.sh ",
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
    >> check_if_weekly
    >> dbt_test
    >> compute_metrics_elementary
    >> create_elementary_report
    >> send_elementary_report
    >> recompile_dbt_project
)
(check_if_weekly >> dbt_test_weekly >> compute_metrics_elementary)
