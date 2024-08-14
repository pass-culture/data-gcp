from datetime import datetime, timedelta

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, ENV_SHORT_NAME
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)

from airflow import DAG
from airflow.models import Param

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"algo-custom-deployment-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/algo_training"

with DAG(
    "algo_custom_deployment",
    default_args=default_args,
    description="ML Custom Deployment job",
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
) as dag:
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task", instance_name=GCE_INSTANCE, retries=2
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.10",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
        dag=dag,
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
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> deploy_model
        >> gce_instance_stop
    )
