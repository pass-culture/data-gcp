import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    GCloudSSHGCEOperator,
    SSHGCEOperator,
)
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME
from common.utils import get_airflow_schedule

GCE_INSTANCE = f"import-batch-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/batch"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "import_batch",
    default_args=default_args,
    description="Import batch push notifications statistics",
    schedule_interval=get_airflow_schedule("0 0 * * *"),  # import every day at 00:00
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "PC-18869-data-import-donnees-batch",
            type="string",
        )
    },
) as dag:

    start = DummyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.10",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
    )

    ios_job = GCloudSSHGCEOperator(
        task_id=f"import_ios",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        python main.py --gcp-project-id={GCP_PROJECT_ID} --env-short-name={ENV_SHORT_NAME} --operating-system=ios      
        """,
    )

    android_job = GCloudSSHGCEOperator(
        task_id=f"import_android",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        python main.py --gcp-project-id={GCP_PROJECT_ID} --env-short-name={ENV_SHORT_NAME} --operating-system=android      
        """,
    )

    gce_instance_stop = StopGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    (start >> gce_instance_start >> fetch_code >> install_dependencies)
    (install_dependencies >> ios_job >> gce_instance_stop)
    (install_dependencies >> android_job >> gce_instance_stop)
