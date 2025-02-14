from datetime import datetime, timedelta

from common.alerts import on_failure_combined_callback
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param

# Configurations
DAG_NAME = "bigquery_export_old_partitions"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/bigquery_archive_partition"
TABLES = {
    "past_offer_context": {
        "dataset_id": f"raw_{ENV_SHORT_NAME}",
        "partition_column": "import_date",
        "look_back_months": {"dev": 1, "stg": 1, "prod": 3},
        "folder": "recommendation",
    },
    "firebase_events": {
        "dataset_id": f"raw_{ENV_SHORT_NAME}",
        "partition_column": "event_date",
        "look_back_months": {"dev": 1, "stg": 3, "prod": 24},
        "folder": "tracking",
    },
}

dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": on_failure_combined_callback,
    "retry_delay": timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

# Define the DAG
dag = DAG(
    DAG_NAME,
    default_args=default_dag_args,
    schedule_interval=SCHEDULE_DICT[DAG_NAME],  # Runs daily
    catchup=False,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_name": Param(
            default=f"bigquery-archive-{ENV_SHORT_NAME}",
            type="string",
        ),
    },
)

gce_instance_start = StartGCEOperator(
    instance_name="{{ params.instance_name }}",
    task_id="gce_start_task",
    labels={"dag_name": DAG_NAME},
    dag=dag,
)

fetch_install_code = InstallDependenciesOperator(
    task_id="fetch_install_code",
    instance_name="{{ params.instance_name }}",
    branch="{{ params.branch }}",
    base_dir=BASE_PATH,
    retries=2,
    dag=dag,
)

tasks = []
for table, config in TABLES.items():
    export_old_partitions_to_gcs = SSHGCEOperator(
        task_id=f"export_old_partitions_to_gcs_{table}",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_PATH,
        environment=dag_config,
        command=f"python main.py --table {table} --config '{config}' ",
        do_xcom_push=True,
    )
    tasks.append(export_old_partitions_to_gcs)


gce_instance_stop = DeleteGCEOperator(
    task_id="gce_stop_task",
    instance_name="{{ params.instance_name }}",
)

gce_instance_start >> fetch_install_code >> tasks >> gce_instance_stop
