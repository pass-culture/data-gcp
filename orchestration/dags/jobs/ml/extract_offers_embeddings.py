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
from dependencies.ml.embeddings import offer_to_extract_embedding

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
    "extract_offers_embeddings",
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
    },
) as dag:

    data_collect_task = BigQueryInsertJobOperator(
        task_id=f"import_batch_to_clean",
        configuration={
            "query": {
                "query": "/dependencies/ml/embeddings/offer_to_extract_embeddings.sql",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "tableId": "sandbox_dev.offer_to_extract_embeddings",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        params=dict({"gcp_project": GCP_PROJECT_ID, "env_short_name": ENV_SHORT_NAME}),
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

    data_collect = GCloudSSHGCEOperator(
        task_id="data_collect",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
        python data_collect.py \
        --gcp-project {GCP_PROJECT_ID} \
        --env-short-name {ENV_SHORT_NAME}
        """,
    )

    preprocess = GCloudSSHGCEOperator(
        task_id="preprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python preprocess.py \
        --gcp-project {GCP_PROJECT_ID} \
        --env-short-name {ENV_SHORT_NAME}
        """,
    )

    record_linkage = GCloudSSHGCEOperator(
        task_id="record_linkage",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python main.py \
        --gcp-project {GCP_PROJECT_ID} \
        --env-short-name {ENV_SHORT_NAME}
        """,
    )

    postprocess = GCloudSSHGCEOperator(
        task_id="postprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python postprocess.py \
        --gcp-project {GCP_PROJECT_ID} \
        --env-short-name {ENV_SHORT_NAME}
        """,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        data_collect_task
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> data_collect
        >> preprocess
        >> record_linkage
        >> postprocess
        >> gce_instance_stop
    )
