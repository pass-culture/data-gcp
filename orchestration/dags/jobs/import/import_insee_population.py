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
    StartGCEOperator,
    UvxGCEOperator,
)
from common.utils import get_airflow_schedule

DAG_NAME = "import_insee_population"
GCE_INSTANCE = f"import-insee-pop-{ENV_SHORT_NAME}"
INSEE_POP_PACKAGE = "passculture-data-insee-population @ git+https://github.com/pass-culture/data-insee-population.git"

schedule_interval = "0 2 1 * *"  # Monthly on the 1st at 2 AM

default_dag_args = {
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 1,
    "on_failure_callback": on_failure_vm_callback,
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import INSEE population data at department, EPCI and IRIS levels",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule(schedule_interval),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
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
    start = EmptyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
        instance_type="n1-standard-8",
    )

    import_insee_population = UvxGCEOperator(
        task_id="import_insee_population",
        instance_name=GCE_INSTANCE,
        package=INSEE_POP_PACKAGE,
        command=(
            "insee-population population"
            " --min-age 15 --max-age 24"
            " --start-year 2020 --end-year 2026"
            " --to-bigquery"
            " --project-id $GCP_PROJECT_ID"
            " --dataset raw_$ENV_SHORT_NAME"
        ),
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name=GCE_INSTANCE,
    )

    end = EmptyOperator(task_id="end")

    (start >> gce_instance_start >> import_insee_population >> gce_instance_stop >> end)
