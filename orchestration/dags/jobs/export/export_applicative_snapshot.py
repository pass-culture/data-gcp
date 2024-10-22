import datetime
from pathlib import Path

from common import macros
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

GCE_INSTANCE = f"historize-applicative-database-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/export_applicative"

dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 6,
    # "on_failure_callback": task_fail_slack_alert,
    "on_failure_callback": None,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

raw_directory = Path(PATH_TO_DBT_PROJECT + "models/intermediate/raw")
sql_files = list(raw_directory.glob("*.sql"))
SNAPSHOTS = {}

dag = DAG(
    "historize_applicative_database",
    default_args=default_dag_args,
    description="historize applicative database",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=480),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=["VM"],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
    },
)

start = DummyOperator(task_id="start", dag=dag)

with TaskGroup(group_id="wait_transfo", dag=dag) as wait:
    for sql_file in sql_files:
        table_name = sql_file.stem
        waiting_task = delayed_waiting_operator(
            dag,
            external_dag_id="dbt_run_dag",
            external_task_id=f"data_transformation.{table_name}",
        )

gce_instance_start = StartGCEOperator(
    instance_name=GCE_INSTANCE,
    task_id="gce_start_task",
    dag=dag,
)
fetch_install_code = InstallDependenciesOperator(
    task_id="fetch_install_code",
    instance_name=GCE_INSTANCE,
    branch="{{ params.branch }}",
    installer="uv",
    python_version="3.10",
    base_dir=BASE_PATH,
    dag=dag,
)
with TaskGroup(group_id="applicatives_to_gcs", dag=dag) as tables_to_gcs:
    for sql_file in sql_files:
        table_name = sql_file.stem
        table_to_gcs = SSHGCEOperator(
            task_id="snapshot_to_gcs",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_PATH,
            environment=dag_config,
            command=f"python main.py {table_name}",
            do_xcom_push=False,
            installer="uv",
            dag=dag,
        )

gce_instance_stop = StopGCEOperator(
    task_id="gce_stop_task",
    instance_name=GCE_INSTANCE,
    dag=dag,
)


(
    start
    >> wait
    >> gce_instance_start
    >> fetch_install_code
    >> tables_to_gcs
    >> gce_instance_stop
)
