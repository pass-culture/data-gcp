import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
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

from common.access_gcp_secrets import access_secret_data

SLACK_CONN_PASSWORD = access_secret_data(
    GCP_PROJECT_ID, "slack-analytics-conn-password"
)
SLACK_WEBHOOK_URL = f"https://hooks.slack.com/services/{SLACK_CONN_PASSWORD}"

default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "dbt_artifacts",
    default_args=default_args,
    catchup=False,
    description="Compute data quality metrics with package elementary and re_data and send Slack notifications reports",
    schedule_interval=get_airflow_schedule("0 3 * * *"),
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
)

# Basic steps
start = DummyOperator(task_id="start", dag=dag)

wait_dbt_run = waiting_operator(dag, "dbt_run_dag")

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test.sh ",
    env={
        "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
        "target": "{{ params.target }}",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

compute_metrics_re_data = BashOperator(
    task_id="compute_metrics_re_data",
    bash_command="dbt run --target {{ params.target }} --select package:re_data --profile data_gcp_dbt "
    + f"--target-path {PATH_TO_DBT_TARGET}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

compute_metrics_elementary = BashOperator(
    task_id="compute_metrics_elementary",
    bash_command="dbt run --target {{ params.target }} --select package:elementary --profile elementary "
    + f"--target-path {PATH_TO_DBT_TARGET}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

with TaskGroup(group_id="re_data", dag=dag) as re_data_overview:
    re_data_generate_json = BashOperator(
        task_id="re_data_generate_json",
        bash_command="""dbt run-operation generate_overview --args '{end_date: '{{ today() }}', start_date: '{{ last_week() }}', interval: 'days:1', monitored_path: """
        + f"{PATH_TO_DBT_TARGET}"
        + "/re_data/monitored.json"
        + ", overview_path: "
        + f"{PATH_TO_DBT_TARGET}"
        + "/re_data/overview.json}' "
        + f"--target {ENV_SHORT_NAME}",
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag,
    )

    export_tests_history = BashOperator(
        task_id="export_tests_history",
        bash_command="dbt run-operation export_tests_history --args '{end_date: '{{ today() }}', start_date: '{{ last_week() }}', tests_history_path: "
        + f"{PATH_TO_DBT_TARGET}"
        + "/re_data/tests_history.json }' "
        + f"--target {ENV_SHORT_NAME}",
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag,
    )

    export_table_samples = BashOperator(
        task_id="export_table_samples",
        bash_command="dbt run-operation export_table_samples --args '{end_date: '{{ today() }}', start_date: '{{ last_week() }}', table_samples_path: "
        + f"""{PATH_TO_DBT_TARGET}"""
        + "/re_data/table_samples_path.json}' "
        + f"--target {ENV_SHORT_NAME}",
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag,
    )


re_data_notify = BashOperator(
    task_id="re_data_notify",
    bash_command=f""" 
    re_data notify slack \
    --target {ENV_SHORT_NAME} \
    --webhook-url  {SLACK_WEBHOOK_URL} \
    --subtitle="More details here : link to <re_data>" \
    --select anomaly \
    --select test \
    --select schema_change
    """,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

(start >> wait_dbt_run >> compute_metrics_re_data >> re_data_overview >> re_data_notify)
wait_dbt_run >> dbt_test
wait_dbt_run >> compute_metrics_elementary
