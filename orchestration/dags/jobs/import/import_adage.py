import datetime
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
from common.operators.sensor import TimeSleepSensor
from common.utils import (
    getting_service_account_token,
    get_airflow_schedule,
)

from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER

from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME

GCE_INSTANCE = f"import-adage-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/adage"

dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}
default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    "import_adage_v1",
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
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.8",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
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

    gce_instance_stop = StopGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    end = DummyOperator(task_id="end", dag=dag)

    (
        sleep_op
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> adage_to_bq
        >> gce_instance_stop
        >> end
    )
