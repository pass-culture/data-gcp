import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
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
from common.utils import get_airflow_schedule
from kubernetes.client import V1ResourceRequirements

DAG_NAME = "import_qualtrics"
MICROSERVICE_PATH = "jobs/etl_jobs/external/qualtrics"
QUALTRICS_ENV_VARS = {"PROJECT_NAME": GCP_PROJECT_ID}
HEAVY_CONTAINER_RESOURCES = V1ResourceRequirements(
    requests={"cpu": "1", "memory": "3Gi"},
    limits={"cpu": "2", "memory": "8Gi"},
)
default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import qualtrics tables",
    schedule=get_airflow_schedule("0 0 * * 1"),  # execute each Monday at midnight
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    _kpo_common = dict(
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        env_vars=QUALTRICS_ENV_VARS,
    )

    import_opt_out_to_bigquery = CustomKubernetesPodOperator(
        task_id="import_opt_out_to_bigquery",
        arguments=["main.py", "--task", "import_opt_out_users"],
        deferrable=True,
        container_resources=DEFAULT_CONTAINER_RESOURCES,
        **_kpo_common,
    )

    import_all_answers_to_bigquery = CustomKubernetesPodOperator(
        task_id="import_all_answers_to_bigquery",
        arguments=["main.py", "--task", "import_all_survey_answers"],
        deferrable=True,
        container_resources=HEAVY_CONTAINER_RESOURCES,
        **_kpo_common,
    )

    import_opt_out_to_bigquery
    import_all_answers_to_bigquery
