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
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule
from kubernetes.client import V1ResourceRequirements

DAG_NAME = "import_zendesk"
MICROSERVICE_PATH = "jobs/etl_jobs/external/zendesk"
ZENDESK_CONTAINER_RESOURCES = V1ResourceRequirements(
    requests={"cpu": "1", "memory": "2Gi"},
    limits={"cpu": "2", "memory": "4Gi"},
)


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Zendesk data into BigQuery",
    schedule=get_airflow_schedule("30 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "ndays": Param(
            default=7 if ENV_SHORT_NAME == "prod" else 1,
            type="integer",
        ),
        "job": Param(
            default="both",
            enum=[
                "macro_stat",
                "ticket_stat",
                "survey_response_stat",
                "both",
            ],
            type="string",
            help="Specify the job to run: 'macro_stat', 'ticket_stat', 'open_ticket_stat', 'survey_response_stat', or 'both'.",
        ),
        "prior_date": Param(
            default=datetime.datetime.now().strftime("%Y-%m-%d"),
            type="string",
            help="Optional prior date (YYYY-MM-DD) to calculate the ndays range from instead of now().",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    import_data_to_bigquery = CustomKubernetesPodOperator(
        task_id="import_to_bigquery",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=[
            "main.py",
            "--ndays",
            "{{ params.ndays }}",
            "--job",
            "{{ params.job }}",
            "--prior-date",
            "{{ params.prior_date }}",
        ],
        container_resources=ZENDESK_CONTAINER_RESOURCES,
    )
