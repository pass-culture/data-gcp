import json
from datetime import datetime, timedelta

from common.callback import on_failure_vm_callback
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param

# Configurations
DAG_NAME = "bigquery_archive_partition"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/bigquery_archive_partition"
TABLES = {
    "raw_tracking_firebase_events": {
        "table_id": "firebase_events",
        "dataset_id": f"raw_{ENV_SHORT_NAME}",
        "partition_column": "event_date",
        "look_back_months": {"dev": 1, "stg": 3, "prod": 24}[ENV_SHORT_NAME],
        "folder": "tracking",
        "archive": True if ENV_SHORT_NAME == "prod" else False,
    },
    "int_firebase_native_event": {
        "table_id": "native_event",
        "dataset_id": f"int_firebase_{ENV_SHORT_NAME}",
        "partition_column": "event_date",
        "look_back_months": {"dev": 1, "stg": 3, "prod": 12}[ENV_SHORT_NAME],
        "folder": "int_firebase",
        "archive": False,
    },
    "int_firebase_native_event_flattened": {
        "table_id": "native_event_flattened",
        "dataset_id": f"int_firebase_{ENV_SHORT_NAME}",
        "partition_column": "event_date",
        "look_back_months": {"dev": 1, "stg": 3, "prod": 12}[ENV_SHORT_NAME],
        "folder": "int_firebase",
        "archive": False,
    },
    "raw_api_reco_past_offer_context": {
        "table_id": "past_offer_context",
        "dataset_id": f"raw_{ENV_SHORT_NAME}",
        "partition_column": "import_date",
        "look_back_months": {"dev": 1, "stg": 3, "prod": 6}[ENV_SHORT_NAME],
        "folder": "api_reco",
        "archive": True if ENV_SHORT_NAME == "prod" else False,
    },
    "int_pcreco_past_offer_context": {
        "table_id": "past_offer_context",
        "dataset_id": f"int_pcreco_{ENV_SHORT_NAME}",
        "partition_column": "event_date",
        "look_back_months": {"dev": 1, "stg": 3, "prod": 6}[ENV_SHORT_NAME],
        "folder": "api_reco",
        "archive": False,
    },
}

dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": on_failure_vm_callback,
    "retry_delay": timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    DAG_NAME,
    default_args=default_dag_args,
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT.get(DAG_NAME, None)),
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
        "limit": Param(
            default=30,
            type="integer",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
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
for job_name, config in TABLES.items():
    config = json.dumps(config)
    export_old_partitions_to_gcs = SSHGCEOperator(
        task_id=f"export_old_partitions_to_gcs_{job_name}",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_PATH,
        environment=dag_config,
        command=f"python main.py --config '{config}' --limit {{{{ params.limit }}}}",
        do_xcom_push=True,
    )
    tasks.append(export_old_partitions_to_gcs)


gce_instance_stop = DeleteGCEOperator(
    task_id="gce_stop_task",
    instance_name="{{ params.instance_name }}",
)

gce_instance_start >> fetch_install_code >> tasks >> gce_instance_stop
