from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    GCloudSSHGCEOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, DAG_FOLDER
from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"extract-offers-embeddings-{ENV_SHORT_NAME}"
BASE_DIR = f"data-gcp/embeddings"

default_args = {
    "start_date": datetime(2023, 3, 6),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "embeddings_extraction_offers",
    default_args=default_args,
    description="Extact offer metadata embeddings",
    schedule_interval=get_airflow_schedule("0 0 * * 0"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production"
            if ENV_SHORT_NAME == "prod"
            else "PC-20771-extract-and-import-offers-metadata-emb-to-BQ",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-32",
            type="string",
        ),
        "config_file_name": Param(
            default="default-config-offer",
            type="string",
        ),
    },
) as dag:

    data_collect_task = BigQueryInsertJobOperator(
        task_id=f"import_batch_to_clean",
        configuration={
            "query": {
                "query": "{% include '/dependencies/ml/embeddings/offer_to_extract_embedding.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": f"clean_{ENV_SHORT_NAME}",
                    "tableId": "offer_to_extract_embeddings",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        dag=dag,
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code", instance_name=GCE_INSTANCE, command="{{ params.branch }}"
    )

    install_dependencies = GCloudSSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
    )

    preprocess = GCloudSSHGCEOperator(
        task_id="preprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python preprocess.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} ",
    )

    extract_embedding = GCloudSSHGCEOperator(
        task_id="extract_embedding",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python main.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} ",
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
        >> gce_instance_stop
    )
