from datetime import datetime, timedelta

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

from airflow import DAG
from airflow.models import Param

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = "algo-custom-deployment"
BASE_DIR = "data-gcp/jobs/ml_jobs/algo_training"
DAG_NAME = "algo_custom_deployment"

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="ML Custom Deployment job",
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "experiment_name": Param(
            default=f"algo_training_version_b_{ENV_SHORT_NAME}", type="string"
        ),
        "run_id": Param(default=".", type="string"),
        "endpoint_name": Param(
            default=f"recommendation_version_b_{ENV_SHORT_NAME}", type="string"
        ),
        "version_name": Param(default="v_YYYYMMDD", type="string"),
        "default_region": Param(default=DEFAULT_REGION, type="string"),
        "instance_type": Param(default="n1-standard-2", type="string"),
    },
):
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        retries=2,
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_DIR,
        retries=2,
    )

    template_command = r"""
        python deploy_model.py \
            --region {{ params.default_region }} \
            --experiment-name {{ params.experiment_name }} \
            --run-id {{ params.run_id }} \
            --endpoint-name {{ params.endpoint_name }} \
            --version-name {{ params.version_name }} \
            --instance-type {{ params.instance_type }}
    """

    deploy_model = SSHGCEOperator(
        task_id="deploy_custom_model",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=template_command,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (gce_instance_start >> fetch_install_code >> deploy_model >> gce_instance_stop)
