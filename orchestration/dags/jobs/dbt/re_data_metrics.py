import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime, timedelta
from airflow.models import Param
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)

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
    "re_data_metrics",
    default_args=default_args,
    catchup=False,
    description="Compute data quality metrics and send Slack notifications reports",
    schedule_interval=get_airflow_schedule("0 3 * * *"),
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
    },
)

# Basic steps
start = DummyOperator(task_id="start", dag=dag)

compute_metrics = BashOperator(
    task_id="compute_metrics",
    bash_command="dbt run --target {{ params.target }} --select package:re_data"
    + f"--target-path {PATH_TO_DBT_TARGET}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

re_data_generate_json = BashOperator(
    task_id="re_data_generate_json",
    bash_command="re_data overview generate" + f"--target-path {PATH_TO_DBT_TARGET}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

re_data_notify = BashOperator(
    task_id="re_data_notify",
    bash_command=f""" 
    re_data notify slack \
    --webhook-url  {SLACK_WEBHOOK_URL} \
    --subtitle="More details here : link to <re_data>" \
    --select anomaly \
    --select test \
    --select schema_change
    """,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

start >> compute_metrics >> re_data_generate_json >> re_data_notify
