import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from common.utils import get_airflow_schedule
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from common.alerts import analytics_fail_slack_alert
from common.config import DAG_FOLDER, GCP_PROJECT_ID
from airflow.operators.python import PythonOperator

from dependencies.great_expectations.run_tests import (
    run_applicative_history_tests,
    run_enriched_tests,
)
from dependencies.great_expectations.config_historical import (
    historical_applicative_test_config,
)

from dependencies.great_expectations.config_enriched import (
    enriched_tables_test_config,
)
from dependencies.great_expectations.utils import clear_directory

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "tests_great_expectations",
    default_args=default_dag_args,
    description="Perform tests with Great Expectations",
    schedule_interval=get_airflow_schedule("0 6 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

clear_directory_checkpoint = PythonOperator(
    task_id="clear_directory_checkpoint",
    python_callable=clear_directory,
    op_kwargs={"path": "dags/great_expectations/", "directory_name": "checkpoints"},
    dag=dag,
)

clear_directory_expectations = PythonOperator(
    task_id="clear_directory_expectations",
    python_callable=clear_directory,
    op_kwargs={"path": "dags/great_expectations/", "directory_name": "expectations"},
    dag=dag,
)


create_context_applicative_history_tests = PythonOperator(
    task_id="create_context_applicative_history_tests",
    python_callable=run_applicative_history_tests,
    dag=dag,
)

create_context_enriched_tests = PythonOperator(
    task_id="create_context_enriched_tests",
    python_callable=run_enriched_tests,
    dag=dag,
)

ge_tasks_history = []
for table in historical_applicative_test_config.keys():
    ge_task = GreatExpectationsOperator(
        task_id=f"checkpoint_{table}",
        data_context_root_dir="dags/great_expectations/",
        checkpoint_name=f"volume_checkpoint_for_{table}",
        dag=dag,
    )

    ge_tasks_history.append(ge_task)

ge_tasks_enriched = []
for table in enriched_tables_test_config.keys():
    ge_task = GreatExpectationsOperator(
        task_id=f"checkpoint_{table}",
        data_context_root_dir="dags/great_expectations/",
        checkpoint_name=f"freshness_checkpoint_for_{table}",
        dag=dag,
    )

    ge_tasks_enriched.append(ge_task)


(
    start
    >> clear_directory_checkpoint
    >> clear_directory_expectations
    >> create_context_applicative_history_tests
    >> ge_tasks_history
)
clear_directory_expectations >> create_context_enriched_tests >> ge_tasks_enriched
