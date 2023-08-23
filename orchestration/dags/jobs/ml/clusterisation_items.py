from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.operators.biquery import bigquery_job_task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dependencies.ml.clusterisation.import_data import params
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    SLACK_CONN_ID,
    SLACK_CONN_PASSWORD,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_TMP_DATASET,
)
from jobs.ml.constants import IMPORT_CLUSTERING_SQL_PATH

from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"clusterisation-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/clusterisation"
DATE = "{{ yyyymmdd(ds) }}"
STORAGE_PATH = (
    f"gs://{DATA_GCS_BUCKET_NAME}/clusterisation_{ENV_SHORT_NAME}/clusterisation_{DATE}"
)
default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag_config = {
    "TOKENIZERS_PARALLELISM": "false",
}
with DAG(
    "clusterisation_item",
    default_args=default_args,
    description="Cluster offers from metadata embeddings",
    schedule_interval=get_airflow_schedule("0 0 * * 0"),
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
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-highmem-32",
            type="string",
        ),
        "config_file_name": Param(
            default="default-config",
            type="string",
        ),
        "clusterisation_top_N_items": Param(
            default=10000 if ENV_SHORT_NAME == "prod" else 1000,
            type="integer",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)

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
        f"--input-table {DATE}_items_clusterisation_raw --output-table {DATE}_item_full_encoding_enriched "
        "--config-file-name {{ params.config_file_name }} ",
    )

    clusterisation = SSHGCEOperator(
        task_id="clusterisation",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command="PYTHONPATH=. python clusterisation.py "
        f"--input-table {DATE}_item_full_encoding_enriched --output-table item_clusters "
        "--config-file-name {{ params.config_file_name }} ",
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> data_collect_task
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> preprocess
        >> clusterisation
        >> gce_instance_stop
        >> end
    )
