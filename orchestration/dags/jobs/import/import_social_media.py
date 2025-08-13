import datetime

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
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

from airflow import DAG
from airflow.models import Param

DAG_NAME = "import_social_network"
default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}

schedule_dict = {
    "prod": "0 2 * * *",
    "stg": "0 3 * * *",
    "dev": None,
}[ENV_SHORT_NAME]


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Social Network Data",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule(schedule_dict),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=240),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "n_days": Param(
            default=-14,
            type="integer",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
):
    for social_network in ["tiktok", "instagram"]:
        gce_instance = f"import-{social_network}-{ENV_SHORT_NAME}"
        base_path = f"data-gcp/jobs/etl_jobs/external/{social_network}"
        gce_instance_start = StartGCEOperator(
            instance_name=gce_instance,
            task_id=f"{social_network}_gce_start_task",
            labels={"dag_name": DAG_NAME},
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id=f"{social_network}_fetch_install_code",
            instance_name=gce_instance,
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=base_path,
            retries=2,
        )

        job_to_bq = SSHGCEOperator(
            task_id=f"{social_network}_to_bq",
            instance_name=gce_instance,
            base_dir=base_path,
            command="""
            python main.py \
            --start-date {{ add_days(yesterday() if dag_run.run_type == 'manual' else ds, params.n_days) }} \
            --end-date {{ yesterday() if dag_run.run_type == 'manual' else ds }}
            """,
            do_xcom_push=True,
        )

        gce_instance_stop = DeleteGCEOperator(
            task_id=f"{social_network}_gce_stop_task", instance_name=gce_instance
        )

        (gce_instance_start >> fetch_install_code >> job_to_bq >> gce_instance_stop)
