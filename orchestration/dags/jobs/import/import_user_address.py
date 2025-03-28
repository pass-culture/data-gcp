from datetime import datetime, timedelta

from common import macros
from common.alerts import on_failure_combined_callback
from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

GCE_INSTANCE = f"import-user-address-bulk-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/api_gouv"
DAG_NAME = "import_user_address_bulk"

dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}


schedule_interval = "0 */6 * * *" if ENV_SHORT_NAME == "prod" else "30 2 * * *"

default_args = {
    "start_date": datetime(2021, 3, 30),
    "on_failure_callback": on_failure_combined_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Importing new data from addresses api every day.",
    schedule_interval=get_airflow_schedule(schedule_interval),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_name": Param(
            default=GCE_INSTANCE,
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
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
            return "start_gce"
        return "end"

    @task
    def start_gce():
        context = get_current_context()

        # Instantiate the operator with templated fields
        operator = StartGCEOperator(
            instance_name="{{ params.instance_name }}",
            task_id="gce_start_task",
            labels={"dag_name": DAG_NAME},
        )
        # Render all template fields in the operator
        operator.render_template_fields(context)
        # Use run() so that Airflow handles logging and hooks properly
        return operator.run(context=context)

    @task
    def fetch_install_code():
        context = get_current_context()

        operator = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name="{{ params.instance_name }}",
            branch="{{ params.branch }}",
            python_version="3.12",
            base_dir=BASE_PATH,
        )
        operator.render_template_fields(context)
        return operator.run(context=context)

    @task
    def addresses_to_gcs():
        context = get_current_context()

        operator = SSHGCEOperator(
            task_id="user_address_to_bq",
            instance_name=GCE_INSTANCE,  # if this is a constant, no templating needed
            base_dir=BASE_PATH,
            environment=dag_config,
            command="""python main.py \
                --source-dataset-id {{ params.source_dataset_id }} \
                --source-table-name {{ params.source_table_name }} \
                --destination-dataset-id {{ params.destination_dataset_id }} \
                --destination-table-name {{ params.destination_table_name }} \
                --max-rows {{ params.max_rows }} \
                --chunk-size {{ params.chunk_size }}
            """,
            do_xcom_push=True,
        )
        operator.render_template_fields(context)
        return operator.run(context=context)

    @task
    def stop_gce():
        context = get_current_context()

        operator = DeleteGCEOperator(
            task_id="gce_stop_task",
            instance_name="{{ params.instance_name }}",
        )
        operator.render_template_fields(context)
        return operator.run(context=context)

    @task
    def end():
        return "completed"

    # Define the task dependencies
    start_result = start()
    count_result = check_source_count()
    branch_result = branch(count_result)
    gce_start_result = start_gce()
    fetch_result = fetch_install_code()
    addresses_result = addresses_to_gcs()
    stop_result = stop_gce()
    end_result = end()

    # Set task dependencies
    start_result >> count_result >> branch_result
    (
        branch_result
        >> gce_start_result
        >> fetch_result
        >> addresses_result
        >> stop_result
        >> end_result
    )
    branch_result >> end_result
