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
from dependencies.ml.linkage.import_items import params
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, DAG_FOLDER
from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"link-offers-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/record_linkage"

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
    data_collect = bigquery_job_task(dag, "import_item_batch", params, extra_params={})

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        labels={"job_type": "ml"},
        preemptible=False,
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        python_version="3.10",
        command="{{ params.branch }}",
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
        command=f"""
         python preprocess.py \
        --gcp-project {GCP_PROJECT_ID} \
        --env-short-name {ENV_SHORT_NAME}
        """,
    )

    record_linkage = SSHGCEOperator(
        task_id="record_linkage",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
         python main.py \
        --gcp-project {GCP_PROJECT_ID} \
        --env-short-name {ENV_SHORT_NAME}
        """,
    )

    postprocess = SSHGCEOperator(
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
        data_collect
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> preprocess
        >> record_linkage
        >> postprocess
        >> gce_instance_stop
    )
