from common import macros
from common.alerts import dbt_test_slack_alert, task_fail_slack_alert
from common.config import (
    DATA_GCS_BUCKET_NAME,
    ELEMENTARY_PYTHON_PATH,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
    SLACK_TOKEN_ELEMENTARY,
)
from common.dbt.utils import load_json_artifact, load_manifest
from common.utils import get_airflow_schedule, waiting_operator

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime, timedelta

SLACK_CHANNEL = "alertes-data-quality"
yyyy = ""
yyyymmdd = ""

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
    schedule_interval=get_airflow_schedule("0 1 * * *"),
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

compute_metrics_elementary = BashOperator(
    task_id="compute_metrics_elementary",
    bash_command="dbt run --no-write-json --target {{ params.target }} --select package:elementary --profile elementary "
    + f"--target-path {PATH_TO_DBT_TARGET}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

# if ENV_SHORT_NAME == "prod":
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test.sh ",
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

load_run_results = PythonOperator(
    task_id="load_run_results",
    python_callable=load_json_artifact,
    op_kwargs={
        "_PATH_TO_DBT_TARGET": f"{PATH_TO_DBT_TARGET}",
        "artifact": "run_results.json",
    },
    do_xcom_push=True,
    dag=dag,
)

load_dbt_manifest = PythonOperator(
    task_id="load_dbt_manifest",
    python_callable=load_manifest,
    op_kwargs={
        "_PATH_TO_DBT_TARGET": f"{PATH_TO_DBT_TARGET}",
    },
    do_xcom_push=True,
    dag=dag,
)

warning_alert_slack = PythonOperator(
    task_id="warning_alert_slack",
    python_callable=dbt_test_slack_alert,
    op_kwargs={
        "results_json": "{{task_instance.xcom_pull(task_ids='load_run_results', key='return_value')}}",
        "manifest_json": "{{task_instance.xcom_pull(task_ids='load_dbt_manifest', key='return_value')}}",
    },
    provide_context=True,
    dag=dag,
)

send_elementary_report = BashOperator(
    task_id="send_elementary_report",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/elementary_send_report.sh ",
    env={
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        "PATH_TO_DBT_PROJECT": PATH_TO_DBT_PROJECT,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "DATA_BUCKET_NAME": DATA_GCS_BUCKET_NAME,
        "REPORT_FILE_PATH": "elementary_reports/{{ execution_date.year }}/elementary_report_{{ execution_date.strftime('%Y%m%d') }}.html",
        "SLACK_TOKEN": SLACK_TOKEN_ELEMENTARY,
        "CHANNEL_NAME": SLACK_CHANNEL,
        "ELEMENTARY_PYTHON_PATH": ELEMENTARY_PYTHON_PATH,
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)


# if ENV_SHORT_NAME == "prod":
(
    start
    >> wait_dbt_run
    >> dbt_test
    >> compute_metrics_elementary
    >> (load_run_results, load_dbt_manifest)
    >> warning_alert_slack
    >> send_elementary_report
)
