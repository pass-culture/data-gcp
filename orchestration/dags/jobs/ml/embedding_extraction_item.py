from datetime import datetime, timedelta

from common.callback import on_failure_vm_callback
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    GCP_REGION,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"extract-items-embeddings-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/ml_jobs/embeddings"
DATE = "{{ yyyymmdd(ds) }}"
default_args = {
    "start_date": datetime(2023, 9, 6),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
DAG_CONFIG = {
    "TOKENIZERS_PARALLELISM": "false",
}


INPUT_DATASET_NAME = f"ml_input_{ENV_SHORT_NAME}"
INPUT_TABLE_NAME = "item_embedding_extraction"
OUTPUT_DATASET_NAME = f"ml_preproc_{ENV_SHORT_NAME}"
OUTPUT_TABLE_NAME = "item_embedding_extraction"
DAG_NAME = "embeddings_extraction_item"


@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    description="Extract items metadata embeddings",
    schedule_interval="0 12,18,23 * * *",  # every day at 12:00, 18:00, and 23:00
    catchup=False,
    dagrun_timeout=timedelta(hours=20),
    user_defined_macros=None,  # Replace with actual macros if needed
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
)
def extract_embedding_item_dag(
    branch="production" if ENV_SHORT_NAME == "prod" else "master",
    instance_type="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-32",
    config_file_name="default-config-item",
    batch_size=5000,
    max_rows_to_process=100000 if ENV_SHORT_NAME == "prod" else 15000,
):
    @task
    def start():
        return "started"

    @task
    def check_source_count():
        bq_hook = BigQueryHook(location=GCP_REGION, use_legacy_sql=False)
        bq_client = bq_hook.get_client(project_id=GCP_PROJECT_ID)
        dataset_id = INPUT_DATASET_NAME
        table_name = INPUT_TABLE_NAME
        query = f"""
        SELECT count(*) as count
        FROM `{GCP_PROJECT_ID}`.`{dataset_id}`.`{table_name}`
        """

        result = bq_client.query(query).to_dataframe()
        return result["count"].values[0] > 0

    @task
    def end():
        return "completed"

    @task.branch
    def decide_next_step(has_data: bool):
        if has_data:
            return "start_gce"
        return "end"

    # Use operator classes directly instead of @task decorators
    start_gce_op = StartGCEOperator(
        task_id="start_gce",
        instance_name=GCE_INSTANCE,
        preemptible=False,
        instance_type=instance_type,
        retries=2,
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    fetch_install_code_op = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch=branch,
        python_version="3.10",
        base_dir=BASE_PATH,
    )

    extract_embedding_op = SSHGCEOperator(
        task_id="extract_embedding",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=DAG_CONFIG,
        command="mkdir -p img && PYTHONPATH=. python main.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--config-file-name {config_file_name} "
        f"--batch-size {batch_size} "
        f"--max-rows-to-process {max_rows_to_process} "
        f"--input-dataset-name {INPUT_DATASET_NAME} "
        f"--input-table-name {INPUT_TABLE_NAME} "
        f"--output-dataset-name {OUTPUT_DATASET_NAME} "
        f"--output-table-name {OUTPUT_TABLE_NAME} ",
        deferrable=True,
        poll_interval=300,
    )

    stop_gce_op = DeleteGCEOperator(task_id="stop_gce", instance_name=GCE_INSTANCE)

    # Define task flow
    start_dag = start()
    source_has_data = check_source_count()
    branch_decision = decide_next_step(source_has_data)
    pipeline_completed = end()

    # Set task dependencies
    (
        start_dag
        >> source_has_data
        >> branch_decision
        >> [start_gce_op, pipeline_completed]
    )
    (
        start_gce_op
        >> fetch_install_code_op
        >> extract_embedding_op
        >> stop_gce_op
        >> pipeline_completed
    )


# Instantiate the DAG
extract_embedding_item_dag()
