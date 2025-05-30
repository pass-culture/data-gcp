from datetime import datetime, timedelta

from common.callback import on_failure_vm_callback
from common.config import (
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
from common.utils import get_airflow_schedule
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from jobs.crons import SCHEDULE_DICT


DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"extract-items-embeddings-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/ml_jobs/embeddings"

INPUT_DATASET_NAME = f"ml_input_{ENV_SHORT_NAME}"
INPUT_TABLE_NAME = "item_embedding_extraction"
OUTPUT_DATASET_NAME = f"ml_preproc_{ENV_SHORT_NAME}"
OUTPUT_TABLE_NAME = "item_embedding_extraction"
DAG_NAME = "embeddings_extraction_item"
dag_schedule = get_airflow_schedule(SCHEDULE_DICT.get(DAG_NAME))


default_args = {
    "start_date": datetime(2023, 9, 6),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
DAG_CONFIG = {"TOKENIZERS_PARALLELISM": "false"}


@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    description="Extract items metadata embeddings",
    schedule_interval=dag_schedule,
    catchup=False,
    dagrun_timeout=timedelta(hours=20),
    tags=["ds", "vm", "debug"],
)
def extract_embedding_item_dag(
    branch: str = "production" if ENV_SHORT_NAME == "prod" else "master",
    instance_type: str = "n1-standard-2"
    if ENV_SHORT_NAME == "dev"
    else "n1-standard-32",
    config_file_name: str = "default-config-item",
    batch_size: int = 5000,
    max_rows_to_process: int = 100000 if ENV_SHORT_NAME == "prod" else 15000,
):
    start = EmptyOperator(task_id="start")

    @task
    def check_source_count() -> bool:
        bq_hook = BigQueryHook(location=GCP_REGION, use_legacy_sql=False)
        client = bq_hook.get_client(project_id=GCP_PROJECT_ID)
        df = client.query(
            f"""
            SELECT COUNT(*) as cnt
            FROM `{GCP_PROJECT_ID}`.`{INPUT_DATASET_NAME}`.`{INPUT_TABLE_NAME}`
            """
        ).to_dataframe()
        return int(df["cnt"][0]) > 0

    @task.branch(task_id="decide_next_step")
    def decide_next_step(has_data: bool) -> str:
        return "gce_start" if has_data else "end"

    has_data = check_source_count()
    branch_decision = decide_next_step(has_data)

    gce_start = StartGCEOperator(
        task_id="gce_start",
        instance_name=GCE_INSTANCE,
        preemptible=False,
        instance_type=instance_type,
        retries=2,
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    install_deps = InstallDependenciesOperator(
        task_id="install_deps",
        instance_name=GCE_INSTANCE,
        branch=branch,
        python_version="3.10",
        base_dir=BASE_PATH,
    )

    extract_embeddings = SSHGCEOperator(
        task_id="extract_embedding",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=DAG_CONFIG,
        command=(
            "mkdir -p img && PYTHONPATH=. python main.py "
            "--gcp-project {{ params.gcp_project_id }} "
            "--config-file-name {{ params.config_file_name }} "
            "--batch-size {{ params.batch_size }} "
            "--max-rows-to-process {{ params.max_rows_to_process }} "
            "--input-dataset-name {{ params.input_dataset_name }} "
            "--input-table-name {{ params.input_table_name }} "
            "--output-dataset-name {{ params.output_dataset_name }} "
            "--output-table-name {{ params.output_table_name }}"
        ),
        params={
            "gcp_project_id": GCP_PROJECT_ID,
            "config_file_name": config_file_name,
            "batch_size": batch_size,
            "max_rows_to_process": max_rows_to_process,
            "input_dataset_name": INPUT_DATASET_NAME,
            "input_table_name": INPUT_TABLE_NAME,
            "output_dataset_name": OUTPUT_DATASET_NAME,
            "output_table_name": OUTPUT_TABLE_NAME,
        },
        deferrable=True,
        poll_interval=300,
    )

    gce_stop = DeleteGCEOperator(
        task_id="stop_gce",
        instance_name=GCE_INSTANCE,
    )

    end = EmptyOperator(task_id="end")

    start >> has_data >> branch_decision
    branch_decision >> [gce_start, end]
    gce_start >> install_deps >> extract_embeddings >> gce_stop >> end


extract_embedding_item_dag()
