from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import (
    BIGQUERY_RAW_DATASET,
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

MICROSERVICE_PATH = "jobs/etl_jobs/external/api_gouv"
DAG_NAME = "import_user_address_bulk"

schedule = "0 */6 * * *" if ENV_SHORT_NAME == "prod" else "30 2 * * *"

default_args = {
    "start_date": datetime(2021, 3, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Importing new data from addresses api every day.",
    schedule=get_airflow_schedule(schedule),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "source_dataset_id": Param(
            default=f"int_api_gouv_{ENV_SHORT_NAME}",
            type="string",
        ),
        "source_table_name": Param(
            default="user_address_candidate_queue",
            type="string",
        ),
        "destination_dataset_id": Param(
            default=BIGQUERY_RAW_DATASET,
            type="string",
        ),
        "destination_table_name": Param(
            default="user_address",
            type="string",
        ),
        "max_rows": Param(
            default=50_000,
            type="number",
        ),
        "chunk_size": Param(
            default=500,
            type="number",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:

    @task
    def start():
        return "started"

    @task
    def check_source_count(**context):
        bq_hook = BigQueryHook(location="europe-west1", use_legacy_sql=False)
        bq_client = bq_hook.get_client()
        dataset_id = context["params"]["source_dataset_id"]
        table_name = context["params"]["source_table_name"]
        query = f"""
        SELECT count(*) as count
        FROM `{GCP_PROJECT_ID}`.`{dataset_id}`.`{table_name}`
        """

        result = bq_client.query(query).to_dataframe()
        return result["count"].values[0] > 0

    @task.branch
    def branch(count_result):
        if count_result:
            return "import_to_bigquery"
        return "end"

    @task
    def end():
        return "completed"

    import_to_bigquery = CustomKubernetesPodOperator(
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
            "--source-dataset-id",
            "{{ params.source_dataset_id }}",
            "--source-table-name",
            "{{ params.source_table_name }}",
            "--destination-dataset-id",
            "{{ params.destination_dataset_id }}",
            "--destination-table-name",
            "{{ params.destination_table_name }}",
            "--max-rows",
            "{{ params.max_rows }}",
            "--chunk-size",
            "{{ params.chunk_size }}",
        ],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
    )

    # Define the task dependencies
    start_result = start()
    count_result = check_source_count()
    branch_result = branch(count_result)
    end_result = end()

    start_result >> count_result >> branch_result
    branch_result >> import_to_bigquery >> end_result
    branch_result >> end_result
