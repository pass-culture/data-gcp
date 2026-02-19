import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
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

GCE_INSTANCE = f"import-allocine-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/allocine"
DAG_NAME = "import_allocine_movies"

default_args = {
    "start_date": datetime.datetime(2026, 1, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=2),
}

dag_config = {
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Import allocine movies data",
    schedule_interval=get_airflow_schedule("0 0 * * *"),  # import every day at 00:00
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    start = EmptyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.12",
        base_dir=BASE_PATH,
        dag=dag,
        retries=2,
    )

    synchronize_movies = SSHGCEOperator(
        task_id="synchronize_movies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run main.py sync-movies",
    )

    synchronize_posters = SSHGCEOperator(
        task_id="synchronize_posters",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run main.py sync-posters",
    )

    gce_instance_stop = DeleteGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    end = EmptyOperator(task_id="end", dag=dag)

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> [synchronize_movies, synchronize_posters]
        >> gce_instance_stop
        >> end
    )
