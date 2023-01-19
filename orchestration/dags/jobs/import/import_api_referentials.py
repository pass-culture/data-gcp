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
from common.config import GCP_PROJECT_ID, GCE_ZONE, ENV_SHORT_NAME
from common.utils import get_airflow_schedule

GCE_INSTANCE = f"import-api-referentials-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/analytics/scripts/import_api_referentials"

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
        task_id="fetch_code", instance_name=GCE_INSTANCE, command="{{ params.branch }}"
    )

    INSTALL_DEPS = """
        git clone https://github.com/pass-culture/pass-culture-main.git
        cd pass-culture-main
        cp -r api/src/pcapi ..
        cd ..
        conda create -n py310 python=3.10
        conda init zsh
        source ~/.zshrc
        conda activate py310
        pip install -r requirements.txt --user
    """

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=INSTALL_DEPS,
    )

    tasks = []
    for job_type in ["subcategories", "types"]:

        PYTHON_CODE = f"""
            conda init zsh
            source ~/.zshrc
            conda activate py310
            python main.py --job_type={job_type} --gcp_project_id={GCP_PROJECT_ID} --env_short_name={ENV_SHORT_NAME}        
        """

        import_job = GCloudSSHGCEOperator(
            task_id=f"import_{job_type}",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_PATH,
            command=PYTHON_CODE,
        )
        tasks.append(import_job)

    gce_instance_stop = StopGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> tasks
        >> gce_instance_stop
    )
