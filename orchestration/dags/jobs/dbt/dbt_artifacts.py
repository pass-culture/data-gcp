import logging
from functools import partial

from common import macros
from common.callback import on_failure_base_callback
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
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
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import datetime, timedelta

# Import dbt execution functions
from common.dbt.dbt_executors import (
    compile_dbt_with_selector,
    run_dbt_quality_tests,
    run_dbt_with_selector,
)


def should_run_today(ds):
    try:
        next_tasks = ["dbt_test", "dbt_test_weekly", "dbt_test_monthly"]
        execution_date = datetime.strptime(ds, "%Y-%m-%d")
        if execution_date.weekday() != 0:  # 0 is Monday
            next_tasks.remove("dbt_test_weekly")
        if execution_date.day != 1:
            next_tasks.remove("dbt_test_monthly")
        logging.info(f"Next tasks for ds {ds}: {next_tasks}")
        if len(next_tasks) == 0:
            raise ValueError(f"empty next_tasks for ds: {ds}")
        elif len(next_tasks) == 1:
            next_tasks = next_tasks[0]
        return next_tasks
    except ValueError:
        logging.warning(f"Invalid date format for ds: {ds}")


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

start = EmptyOperator(task_id="start", dag=dag)

wait_dbt_run = delayed_waiting_operator(dag=dag, external_dag_id="dbt_run_dag")

# Convert to Python operator
compute_metrics_elementary = PythonOperator(
    task_id="compute_metrics_elementary",
    python_callable=partial(run_dbt_with_selector, "package:elementary"),
    dag=dag,
    trigger_rule="one_success",
)

# Convert to Python operator
dbt_test = PythonOperator(
    task_id="dbt_test",
    python_callable=partial(
        run_dbt_quality_tests,
        select=None,  # No specific selection, will test all
        exclude="audit tag:export tag:weekly tag:monthly",  #
    ),
    dag=dag,
)

check_schedule = BranchPythonOperator(
    task_id="check_scheduled_run",
    python_callable=should_run_today,
    dag=dag,
)

# Convert to Python operator
dbt_test_weekly = PythonOperator(
    task_id="dbt_test_weekly",
    python_callable=partial(
        run_dbt_quality_tests, select="tag:weekly", exclude="audit"
    ),
    dag=dag,
    trigger_rule="all_success",
)

dbt_test_monthly = PythonOperator(
    task_id="dbt_test_monthly",
    python_callable=partial(
        run_dbt_quality_tests, select="tag:monthly", exclude="audit"
    ),
    dag=dag,
    trigger_rule="all_success",
)

create_elementary_report = GenerateElementaryReportOperator(
    task_id="create_elementary_report",
    report_file_path="elementary_reports/{{ execution_date.year }}/elementary_report_{{ execution_date.strftime('%Y%m%d') }}.html",
    days_back=14,
    trigger_rule="none_failed_min_one_success",
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

# Convert to Python operator
recompile_dbt_project = PythonOperator(
    task_id="recompile_dbt_project",
    python_callable=partial(
        compile_dbt_with_selector,
        selector="package:data_gcp_dbt",
        use_tmp_artifacts=False,
    ),
    dag=dag,
    trigger_rule="all_done",
)


# DAG Dependencies
(
    start
    >> wait_dbt_run
    >> check_schedule
    >> dbt_test
    >> compute_metrics_elementary
    >> create_elementary_report
    >> send_elementary_report
    >> recompile_dbt_project
)
(check_schedule >> dbt_test_weekly >> compute_metrics_elementary)
(check_schedule >> dbt_test_monthly >> compute_metrics_elementary)
