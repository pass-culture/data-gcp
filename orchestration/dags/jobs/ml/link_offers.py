from datetime import datetime, timedelta

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, ENV_SHORT_NAME, GCE_UV_INSTALLER
from common.operators.bigquery import bigquery_job_task
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule
from dependencies.ml.linkage.import_items import (
    ANALYTICS_DATASET,
    MAIN_OUTPUT_TABLE,
    POSTPROCESS_OUTPUT_TABLE,
    PREPROCESS_INPUT_TABLE,
    PREPROCESS_OUTPUT_TABLE,
    SQL_IMPORT_PARAMS,
    TMP_DATASET,
)

from airflow import DAG
from airflow.models import Param

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"link-offers-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/ml_jobs/record_linkage"


default_args = {
    "start_date": datetime(2022, 1, 5),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "link_offers",
    default_args=default_args,
    description="Link offers via recordLinkage",
    schedule_interval=get_airflow_schedule("0 0 * * *"),
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
        "batch_size": Param(
            default=20000 if ENV_SHORT_NAME == "prod" else 10000,
            type="integer",
        ),
    },
) as dag:
    data_collect = bigquery_job_task(
        dag, "import_item_batch", SQL_IMPORT_PARAMS, extra_params={}
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        labels={"job_type": "ml"},
        preemptible=False,
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
        installer=GCE_UV_INSTALLER,
    )

    preprocess = SSHGCEOperator(
        task_id="preprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        installer=GCE_UV_INSTALLER,
        command=f"""
         python preprocess.py \
        --input-dataset-name {TMP_DATASET} \
        --input-table-name {PREPROCESS_INPUT_TABLE} \
        --output-dataset-name {TMP_DATASET} \
        --output-table-name {PREPROCESS_OUTPUT_TABLE}
        """,
    )

    record_linkage = SSHGCEOperator(
        task_id="record_linkage",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        installer=GCE_UV_INSTALLER,
        command=f"""
         python main.py \
        --input-dataset-name {TMP_DATASET} \
        --input-table-name {PREPROCESS_OUTPUT_TABLE} \
        --output-dataset-name {TMP_DATASET} \
        --output-table-name {MAIN_OUTPUT_TABLE}
        """,
    )

    postprocess = SSHGCEOperator(
        task_id="postprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        installer=GCE_UV_INSTALLER,
        command=f"""
         python postprocess.py \
        --input-dataset-name {TMP_DATASET} \
        --input-table-name {MAIN_OUTPUT_TABLE} \
        --output-dataset-name {ANALYTICS_DATASET} \
        --output-table-name {POSTPROCESS_OUTPUT_TABLE}
        """,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        data_collect
        >> gce_instance_start
        >> fetch_install_code
        >> preprocess
        >> record_linkage
        >> postprocess
        >> gce_instance_stop
    )
