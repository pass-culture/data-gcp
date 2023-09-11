from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.operators.biquery import bigquery_job_task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dependencies.ml.embeddings.import_items import params
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, DAG_FOLDER
from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"extract-items-embeddings-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/embeddings"
DATE = "{{ yyyymmdd(ds) }}"
default_args = {
    "start_date": datetime(2023, 9, 6),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag_config = {
    "TOKENIZERS_PARALLELISM": "false",
}
with DAG(
    "embeddings_extraction_items",
    default_args=default_args,
    description="Extact items metadata embeddings",
    schedule_interval=get_airflow_schedule("0 */6 * * *"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
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
            default=50000 if ENV_SHORT_NAME == "prod" else 10000,
            type="integer",
        ),
    },
) as dag:
    data_collect_task = bigquery_job_task(
        dag, "import_item_batch", params, extra_params={}
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        retries=2,
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        python_version="3.10",
        command="{{ params.branch }}",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
    )

    preprocess = SSHGCEOperator(
        task_id="preprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python preprocess.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} "
        f"--input-table-name {DATE}_item_to_extract_embeddings "
        f"--output-table-name {DATE}_item_to_extract_embeddings_clean ",
    )

    extract_embedding = SSHGCEOperator(
        task_id="extract_embedding",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command="mkdir -p img && PYTHONPATH=. python main.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} "
        f"--input-table-name {DATE}_item_to_extract_embeddings_clean "
        f"--output-table-name item_embeddings ",
    )

    reduce_dimension = SSHGCEOperator(
        task_id="reduce_dimension",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command="PYTHONPATH=. python dimension_reduction.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} "
        f"--input-table-name item_embeddings "
        f"--output-table-name item_embeddings_reduced ",
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        data_collect_task
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> preprocess
        >> extract_embedding
        >> reduce_dimension
        >> gce_instance_stop
    )
