import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, GCE_ZONE, ENV_SHORT_NAME
from common.utils import get_airflow_schedule

GCE_INSTANCE = f"import-api-referentials-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/import_api_referentials"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "import_api_referentials",
    default_args=default_args,
    description="Continuous update of api model to BQ",
    schedule_interval=get_airflow_schedule("0 0 * * 1"),  # import every monday at 00:00
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
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

    INSTALL_DEPS = """
        git clone https://github.com/pass-culture/pass-culture-main.git
        cd pass-culture-main
        cp -r api/src/pcapi ..
        cd ..
        pip install -r requirements.txt --user
    """

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=INSTALL_DEPS,
    )

    subcategories_job = SSHGCEOperator(
        task_id=f"import_subcategories",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        python main.py --job_type=subcategories --gcp_project_id={GCP_PROJECT_ID} --env_short_name={ENV_SHORT_NAME}        
    """,
    )

    types_job = SSHGCEOperator(
        task_id=f"import_types",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        python main.py --job_type=types --gcp_project_id={GCP_PROJECT_ID} --env_short_name={ENV_SHORT_NAME}        
    """,
    )

    gce_instance_stop = StopGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> subcategories_job
        >> types_job
        >> gce_instance_stop
    )
