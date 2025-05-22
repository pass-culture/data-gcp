import datetime

from common.callback import on_failure_base_callback
from common.config import (
    GCP_PROJECT_ID,
)
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig

default_args = {
    "start_date": datetime.datetime(2020, 12, 23),
    "retries": 6,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_base_callback,
    "on_skipped_callback": on_failure_base_callback,
}

# Initialize the DAG
dag_id = "dbt_run_dag_astro"


# Define initial and final tasks

profile_config = ProfileConfig(
    profile_name="data_gcp_dbt",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/data_gcp_dbt/profiles.yml",
)


my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        "/opt/airflow/dags/data_gcp_dbt",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
    ),
    operator_args={"install_deps": False},  #
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    dag_id=f"{dag_id}",
    default_args={"retries": 2},
)
