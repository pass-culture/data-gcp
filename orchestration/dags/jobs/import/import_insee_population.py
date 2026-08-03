import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    GCP_PROJECT_ID,
)
from common.operators.kubernetes import (
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule
from kubernetes.client import V1ResourceRequirements

INSEE_CONTAINER_RESOURCES = V1ResourceRequirements(
    requests={"cpu": "2", "memory": "8Gi"},
    limits={"cpu": "4", "memory": "16Gi"},
)

DAG_NAME = "import_insee_population"
INSEE_POP_PACKAGE = "passculture-data-insee-population @ git+https://github.com/pass-culture/data-insee-population.git"

schedule = "0 2 1 * *"  # Monthly on the 1st at 2 AM

default_dag_args = {
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import INSEE population data at department, EPCI and IRIS levels",
    on_failure_callback=None,
    schedule=get_airflow_schedule(schedule),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    start = EmptyOperator(task_id="start")

    import_insee_population = CustomKubernetesPodOperator(
        task_id="import_insee_population",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="containerized",
        runtime_image="py313",
        runtime_image_tag="v1",
        on_finish_action="delete_pod",
        cmds=["sh", "-c"],
        arguments=[
            (
                "mkdir -p /tmp/data/cache; "
                "cd /tmp; "
                "command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh; "
                f'uvx --python 3.13 --from "{INSEE_POP_PACKAGE}" '
                "insee-population population"
                " --min-age 15 --max-age 24"
                " --start-year 2020 --end-year 2026"
                " --to-bigquery"
                " --monthly"
                " --yearly-levels epci,canton,iris"
                " --project-id $GCP_PROJECT_ID"
                " --dataset raw_$ENV_SHORT_NAME"
            )
        ],
        container_resources=INSEE_CONTAINER_RESOURCES,
    )

    end = EmptyOperator(task_id="end")

    start >> import_insee_population >> end
