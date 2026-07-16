import datetime

from airflow import DAG
from airflow.models import Param
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
from common.utils import get_airflow_schedule

from orchestration.dags.common.alerts.task_fail import task_fail_slack_alert

MICROSERVICE_PATH = "jobs/etl_jobs/external/metabase-docs"
DAG_NAME = "metabase_docs"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description=(
        "Enrich + reconcile Notion dashboard docs into "
        "int_metabase_<env>.dashboard_documentation"
    ),
    # After import_notion + metabase_governance (08:00) and the daily dbt refresh of asset_catalog.
    schedule=get_airflow_schedule("00 11 * * *") if ENV_SHORT_NAME == "prod" else None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
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
    enrich_dashboard_docs = CustomKubernetesPodOperator(
        task_id="enrich_dashboard_docs",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=["main.py", "enrich"],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )

    enrich_dashboard_docs
