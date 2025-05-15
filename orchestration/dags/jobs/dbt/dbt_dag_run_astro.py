import datetime
import os
from common.callback import on_failure_base_callback
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from common.dbt.utils import dbt_dag_reconstruction, load_and_process_manifest
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
)
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "start_date": datetime.datetime(2020, 12, 23),
    "retries": 6,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_base_callback,
    "on_skipped_callback": on_failure_base_callback,
}
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig


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
        dbt_executable_path=f"/opt/airflow/dbt_venv/bin/dbt",
    ),
    operator_args={"install_deps": False}, #
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    dag_id=f"{dag_id}",
    default_args={"retries": 2},
)

