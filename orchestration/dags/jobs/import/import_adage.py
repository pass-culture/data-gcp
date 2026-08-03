import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.operators.sensor import TimeSleepSensor
from common.utils import (
    get_airflow_schedule,
)

MICROSERVICE_PATH = "jobs/etl_jobs/external/adage"
DAG_NAME = "import_adage_v1"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    DAG_NAME,
    start_date=datetime.datetime(2020, 12, 1),
    default_args=default_dag_args,
    description="Import Adage from API",
    on_failure_callback=None,
    schedule=get_airflow_schedule("0 1 * * *"),
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
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

    adage_to_bq = CustomKubernetesPodOperator(
        task_id="adage_to_bq",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=["main.py"],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    sleep_op >> adage_to_bq >> end
