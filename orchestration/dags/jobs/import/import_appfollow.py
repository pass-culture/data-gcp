import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import BranchPythonOperator
from common import macros
from common.callback import on_failure_vm_callback
from common.config import DAG_FOLDER, DAG_TAGS, ENV_SHORT_NAME, GCP_PROJECT_ID
from common.operators.kubernetes import CustomKubernetesPodOperator
from common.utils import get_airflow_schedule
from kubernetes.client import V1ResourceRequirements

DAG_NAME = "import_appfollow"

APPS = {"ios": "1557887412", "android": "app.passculture.webapp"}

default_dag_args = {
    "start_date": datetime.datetime(2020, 1, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}
schedule_dict = {
    "prod": "0 2 * * 1",
    "stg": "0 3 * * 1",
    "dev": None,
}[ENV_SHORT_NAME]

container_resources = V1ResourceRequirements(
    requests={
        "cpu": "0.2",
        "memory": "500Mi",
    },
    limits={
        "cpu": "0.5",
        "memory": "1Gi",
    },
)


def choose_platform_to_run(**kwargs):
    selected_platform = kwargs["params"]["platform"]
    if selected_platform == "both":
        return [f"appfollow_etl_{platform}" for platform in APPS.keys()]
    return [f"appfollow_etl_{selected_platform}"]


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Appfollow Data",
    on_failure_callback=None,
    schedule=get_airflow_schedule(schedule_dict),
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
            default=-10,
            type="integer",
            description="Number of days to go back from the execution date for the start date (e.g., -1 for yesterday).",
        ),
        "n_index": Param(
            default=0,
            type="integer",
            description="Offset in days from the execution date for the end date (e.g., 0 for the execution date, -1 for yesterday).",
        ),
        "platform": Param(
            default="both",
            type="string",
            enum=["both", "ios", "android"],
            description="Platform to import",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
):
    branch_platform = BranchPythonOperator(
        task_id="branch_platform",
        python_callable=choose_platform_to_run,
    )
    for platform, ext_id in APPS.items():
        task = CustomKubernetesPodOperator(
            task_id=f"appfollow_etl_{platform}",
            orchestration_mode="celery",
            queue="k8s-watcher",
            runtime_mode="gitsynced",
            runtime_branch="{{ params.branch }}",
            runtime_image="py313",
            runtime_image_tag="v1",
            microservice_path="jobs/etl_jobs/external/appfollow",
            arguments=[
                "main.py",
                "--start-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_days) }}",
                "--end-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_index) }}",
                "--ext-id",
                ext_id,
            ],
            container_resources=container_resources,
        )
        branch_platform >> task
