import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from common.utils import get_airflow_schedule
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from common.alerts import analytics_fail_slack_alert
from common.config import DAG_FOLDER, GCP_PROJECT_ID, ENV_SHORT_NAME
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

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
from dependencies.great_expectations.utils import clear_directory, ge_root_dir

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
    op_kwargs={"path": f"{ge_root_dir}", "directory_name": "checkpoints"},
    dag=dag,
)

clear_directory_expectations = PythonOperator(
    task_id="clear_directory_expectations",
    python_callable=clear_directory,
    op_kwargs={"path": f"{ge_root_dir}", "directory_name": "expectations"},
    dag=dag,
)

end_test_history = DummyOperator(task_id="end_test_history", dag=dag)
ge_tasks_history = []
for table_name, config in historical_applicative_test_config.items():
    ge_task = PythonOperator(
        task_id=f"run_applicative_history_tests_{table_name}",
        python_callable=run_applicative_history_tests,
        op_kwargs={"table_name": f"{table_name}", "config": config},
        dag=dag,
    )

    gcs_task = LocalFilesystemToGCSOperator(
        task_id=f"upload_files_{table_name}",
        src=f"{ge_root_dir}/uncommitted/validations/volume_expectation_for_{table_name}/*/*/*",
        dst=f"validations/volume_expectation_for_{table_name}/",
        bucket=f"data-bucket-{ENV_SHORT_NAME}",
        dag=dag,
    )
    ge_task >> gcs_task >> end_test_history
    ge_tasks_history.append(ge_task)

end_tests = DummyOperator(task_id="end_tests", dag=dag)

ge_tasks_enriched = []
for table_name, config in enriched_tables_test_config.items():
    ge_task = PythonOperator(
        task_id=f"run_enriched_tests_{table_name}",
        python_callable=run_enriched_tests,
        op_kwargs={"table_name": f"{table_name}", "config": config},
        dag=dag,
    )

    gcs_task = LocalFilesystemToGCSOperator(
        task_id=f"upload_files_{table_name}",
        src=f"{ge_root_dir}/uncommitted/validations/freshness_expectation_for_{table_name}/*/*/*",
        dst=f"validations/freshness_expectation_for_{table_name}/",
        bucket=f"data-bucket-{ENV_SHORT_NAME}",
        dag=dag,
    )

    ge_task >> gcs_task >> end_tests
    ge_tasks_enriched.append(ge_task)

(
    start
    >> clear_directory_checkpoint
    >> clear_directory_expectations
    >> ge_tasks_history
)
(end_test_history >> ge_tasks_enriched)
