from datetime import datetime, timedelta

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
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
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
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

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Extact items metadata embeddings",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    dagrun_timeout=timedelta(hours=20),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-32",
            type="string",
        ),
        "config_file_name": Param(
            default="default-config-item",
            type="string",
        ),
        "batch_size": Param(
            default=5_000,
            type="integer",
        ),
        "max_rows_to_process": Param(
            default=100_000 if ENV_SHORT_NAME == "prod" else 15_000,
            type="integer",
        ),
    },
) as dag:

    @task
    def start():
        return "started"

    @task
    def check_source_count(**context):
        bq_hook = BigQueryHook(location="europe-west1", use_legacy_sql=False)
        bq_client = bq_hook.get_client()
        dataset_id = INPUT_DATASET_NAME
        table_name = INPUT_TABLE_NAME
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
    def start_gce(**context):
        operator = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            preemptible=False,
            instance_type=context["params"]["instance_type"],
            retries=2,
            labels={"job_type": "ml", "dag_name": DAG_NAME},
        )
        return operator.execute(context={})

    @task
    def fetch_install_code(**context):
        operator = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=GCE_INSTANCE,
            branch=context["params"]["branch"],
            python_version="3.10",
            base_dir=BASE_PATH,
        )
        return operator.execute(context={})

    @task
    def extract_embedding(**context):
        operator = SSHGCEOperator(
            task_id="extract_embedding",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_PATH,
            environment=DAG_CONFIG,
            command="mkdir -p img && PYTHONPATH=. python main.py "
            f"--gcp-project {GCP_PROJECT_ID} "
            f"--config-file-name {context['params']['config_file_name']} "
            f"--batch-size {context['params']['batch_size']} "
            f"--max-rows-to-process {context['params']['max_rows_to_process']} "
            f"--input-dataset-name {INPUT_DATASET_NAME} "
            f"--input-table-name {INPUT_TABLE_NAME} "
            f"--output-dataset-name {OUTPUT_DATASET_NAME} "
            f"--output-table-name {OUTPUT_TABLE_NAME} ",
        )
        return operator.execute(context={})

    @task
    def stop_gce(**context):
        operator = DeleteGCEOperator(
            task_id="gce_stop_task", instance_name=GCE_INSTANCE
        )
        return operator.execute(context={})

    @task
    def end():
        return "completed"

    # Define task dependencies
    start_result = start()
    count_result = check_source_count()
    branch_result = branch(count_result)
    gce_start_result = start_gce()
    fetch_result = fetch_install_code()
    extract_result = extract_embedding()
    stop_result = stop_gce()
    end_result = end()

    # Set task dependencies
    start_result >> count_result >> branch_result
    (
        branch_result
        >> gce_start_result
        >> fetch_result
        >> extract_result
        >> stop_result
        >> end_result
    )
    branch_result >> end_result
