import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.utils import depends_loop, one_line_query, get_airflow_schedule
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from common.alerts import analytics_fail_slack_alert
from common.config import DAG_FOLDER
from airflow.operators.python import PythonOperator

from common.operators.biquery import bigquery_job_task
from common.config import (
    GCP_PROJECT_ID,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
)

from dependencies.great_expectations.table_expectations import (
    run_applicative_history_tests,
    check_directory,
)
from dependencies.great_expectations.config_historical import (
    historical_applicative_test_config,
)

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "check_data",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

check_dir = PythonOperator(
    task_id="check_dir",
    python_callable=check_directory,
    op_kwargs={"ge_root_dir": "dags/great_expectations/"},
    dag=dag,
)

create_context_applicative_history_tests = PythonOperator(
    task_id="create_context_applicative_history_tests",
    python_callable=run_applicative_history_tests,
    dag=dag,
)

ge_tasks = []
for table in historical_applicative_test_config.keys():
    my_ge_task = GreatExpectationsOperator(
        task_id=f"checkpoint_{table}",
        data_context_root_dir="dags/great_expectations/",
        checkpoint_name=f"volume_checkpoint_for_{table}",
        dag=dag,
    )

    ge_tasks.append(my_ge_task)

start >> check_dir >> create_context >> ge_tasks
