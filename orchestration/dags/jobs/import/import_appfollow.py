import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import BranchPythonOperator
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import DAG_FOLDER, DAG_TAGS, ENV_SHORT_NAME, GCP_PROJECT_ID
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "import_appfollow"

APPS = {"ios": "1557887412", "android": "app.passculture.webapp"}

default_dag_args = {
    "start_date": datetime.datetime(2020, 1, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}


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
    schedule=get_airflow_schedule(SCHEDULE_DICT["import_appfollow"][ENV_SHORT_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=240),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        # AppFollow API is billed in credits (500/month): reviews cost 10 per page of
        # 100 reviews, ratings cost 10 + 10 per 30 days of range per app. Scheduled runs
        # only import the last 7 days (~60 credits/run); use start_date/end_date for
        # backfills, ideally one month at a time to spread the credits.
        "start_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            description="Backfill start date (YYYY-MM-DD). Empty: 7 days before the run date.",
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            description="Backfill end date (YYYY-MM-DD). Empty: the run date.",
        ),
        "platform": Param(
            default="both",
            type="string",
            enum=["both", "ios", "android"],
            description="Platform to import",
        ),
        "dataset": Param(
            default="all",
            type="string",
            enum=["all", "reviews", "ratings"],
            description="Data to import: reviews, ratings (store rating history) or all",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
):
    branch_platform = BranchPythonOperator(
        task_id="branch_platform",
        python_callable=choose_platform_to_run,
    )
    previous_task = branch_platform
    for platform, ext_id in APPS.items():
        # Platforms run sequentially: each pod deletes then appends its own app rows
        # in shared tables, so avoid concurrent DML on the same partitions.
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
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ params.start_date or add_days(base, -7) }}",
                "--end-date",
                "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ params.end_date or base }}",
                "--ext-id",
                ext_id,
                "--dataset",
                "{{ params.dataset }}",
            ],
            container_resources=DEFAULT_CONTAINER_RESOURCES,
            trigger_rule="none_failed_min_one_success",
        )
        branch_platform >> task
        previous_task >> task
        previous_task = task
