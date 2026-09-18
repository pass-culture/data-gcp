from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
)
from common.operators.kubernetes import CustomKubernetesPodOperator
from common.utils import get_airflow_schedule
from kubernetes.client import V1ResourceRequirements

MICROSERVICE_PATH = "jobs/etl_jobs/external/geo_referential"
DAG_NAME = "import_geo_referential"

# The sources are yearly, but the job runs monthly to catch a moved URL or a changed file
# format long before the vintage bump, when nobody is watching.
schedule = "0 3 5 * *"

# Latest vintages published by each producer. Bump them when a new one is out: IGN publishes
# the Contours IRIS edition of the year around July, INSEE the COG in February and the EPCI
# composition in March; the density grid and the FRR zoning lag by one to two years. A new COG
# year also needs its page id in the job's utils/config.py::COG_PAGE_IDS.
COG_YEAR = 2026
DENSITY_GRID_YEAR = 2024
FRR_YEAR = 2025

GEO_REFERENTIAL_CONTAINER_RESOURCES = V1ResourceRequirements(
    requests={"cpu": "1", "memory": "2Gi"},
    limits={"cpu": "2", "memory": "4Gi"},
)

default_args = {
    "start_date": datetime(2026, 9, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Import the geographic referential (IRIS, communes, EPCI, zonings) into raw.",
    schedule=get_airflow_schedule(schedule),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "year": Param(
            default=COG_YEAR,
            type="integer",
            description="COG / EPCI / IGN Contours IRIS vintage",
        ),
        "density_year": Param(
            default=DENSITY_GRID_YEAR,
            type="integer",
            description="INSEE density grid vintage",
        ),
        "frr_year": Param(
            default=FRR_YEAR,
            type="integer",
            description="FRR zoning vintage",
        ),
        "destination_dataset_id": Param(
            default=BIGQUERY_RAW_DATASET,
            type="string",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    start = EmptyOperator(task_id="start")

    import_geo_referential = CustomKubernetesPodOperator(
        task_id="import_geo_referential",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=[
            "main.py",
            "--year",
            "{{ params.year }}",
            "--density-year",
            "{{ params.density_year }}",
            "--frr-year",
            "{{ params.frr_year }}",
            "--destination-dataset-id",
            "{{ params.destination_dataset_id }}",
        ],
        container_resources=GEO_REFERENTIAL_CONTAINER_RESOURCES,
    )

    end = EmptyOperator(task_id="end")

    start >> import_geo_referential >> end
