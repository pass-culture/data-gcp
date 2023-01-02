from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    DAG_FOLDER,
)

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"link-offers-{ENV_SHORT_NAME}"
BASE_DIR = f"data-gcp/record_linkage"

default_args = {
    "start_date": datetime(2022, 12, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "link_offers",
    default_args=default_args,
    description="Link offers via recordLinkage",
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "machine_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-32",
            type="string",
        ),
    },
) as dag:

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        machine_type="{{ params.machine_type }}",
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code", instance_name=GCE_INSTANCE, branch="{{ params.branch }}"
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
    )

    data_collect = SSHGCEOperator(
        task_id="data_collect",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
        python data_collect.py \
        --gcp-project {GCP_PROJECT_ID} \
        --env-short-name {ENV_SHORT_NAME}
        """,
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
        task_id="gce_stop_task",
        instance_name=GCE_INSTANCE,
    )

    (
        gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> data_collect
        >> preprocess
        >> record_linkage
        >> postprocess
        >> gce_instance_stop
    )
