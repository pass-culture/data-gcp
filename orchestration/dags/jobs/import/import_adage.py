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
from common.operators.sensor import TimeSleepSensor
from common.utils import (
    get_airflow_schedule,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

GCE_INSTANCE = f"import-adage-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/adage"
DAG_NAME = "import_adage_v1"

dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}
default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    DAG_NAME,
    start_date=datetime.datetime(2020, 12, 1),
    default_args=default_dag_args,
    description="Import Adage from API",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    # Cannot Schedule before 5AM UTC+2 as data from API is not available.
    sleep_op = TimeSleepSensor(
        dag=dag,
        task_id="sleep_task",
        execution_delay=datetime.timedelta(days=1),  # Execution Date = day minus 1
        sleep_duration=datetime.timedelta(minutes=120),  # 2H
        poke_interval=3600,  # check every hour
        mode="reschedule",
    )

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.8",
        base_dir=BASE_PATH,
        dag=dag,
        retries=2,
    )

    adage_to_bq = SSHGCEOperator(
        task_id="adage_to_bq",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
    )

    gce_instance_stop = DeleteGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    end = EmptyOperator(task_id="end", dag=dag)

    (
        sleep_op
        >> gce_instance_start
        >> fetch_install_code
        >> adage_to_bq
        >> gce_instance_stop
        >> end
    )
