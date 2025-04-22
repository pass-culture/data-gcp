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

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

GCE_INSTANCE = f"import-api-referentials-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/import_api_referentials"
DAG_NAME = "import_api_referentials"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Continuous update of api model to BQ",
    schedule_interval=get_airflow_schedule(
        "30 0 * * 1"
    ),  # import every monday at 00:00
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    start = DummyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.11",
        base_dir=BASE_PATH,
    )

    subcategories_job = SSHGCEOperator(
        task_id="import_subcategories",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        CORS_ALLOWED_ORIGINS="" CORS_ALLOWED_ORIGINS_NATIVE="" CORS_ALLOWED_ORIGINS_AUTH="" CORS_ALLOWED_ORIGINS_ADAGE_IFRAME="" python main.py --job_type=subcategories --gcp_project_id={GCP_PROJECT_ID} --env_short_name={ENV_SHORT_NAME}
    """,
    )

    types_job = SSHGCEOperator(
        task_id="import_types",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        CORS_ALLOWED_ORIGINS="" CORS_ALLOWED_ORIGINS_NATIVE="" CORS_ALLOWED_ORIGINS_AUTH="" CORS_ALLOWED_ORIGINS_ADAGE_IFRAME="" python main.py --job_type=types --gcp_project_id={GCP_PROJECT_ID} --env_short_name={ENV_SHORT_NAME}
    """,
    )

    gce_instance_stop = DeleteGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> subcategories_job
        >> types_job
        >> gce_instance_stop
    )
