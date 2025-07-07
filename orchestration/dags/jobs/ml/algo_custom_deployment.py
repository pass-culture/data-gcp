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
GCE_INSTANCE = f"algo-custom-deployment-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/algo_training"
DAG_NAME = "algo_custom_deployment"

DAG_DOC = """
    ### Deploy training model from artifact registry to VertexAI Online prediction endpoint
    - The model has to be registered on mlflow.
    - Go to `mlflow_training_results` table in BigQuery:
    - Get `experiment_name` and `run_id` of the desired model.

    #### Parameters:
    * `branch`: you can leave it to production if you did not change the deployment code.
    * `default_region`: leave it as europe-west1.
    * `endpoint_name`: name of the endpoint on VertexAI Online prediction.
    * `experiment_name`: experiment_name from mlflow_training_results table.
    * `instance_type`: choose between n1-standard-{2,4,8,16}
    * `run_id`: run_id from mlflow_training_results table.
    * `version_name`: choose the version (e.g.: v_YYYYMMDD).
    """

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
            default=f"dummy_version_x_{ENV_SHORT_NAME}", type="string"
        ),
        "run_id": Param(default=".", type="string"),
        "endpoint_name": Param(
            default=f"dummy_version_x_{ENV_SHORT_NAME}", type="string"
        ),
        "version_name": Param(default="v_YYYYMMDD", type="string"),
        "default_region": Param(default=DEFAULT_REGION, type="string"),
        "instance_name": Param(default=GCE_INSTANCE, type="string"),
        "instance_type": Param(default="n1-standard-2", type="string"),
    },
    doc_md=DAG_DOC,
):
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name="{{ params.instance_name }}",
        retries=2,
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
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
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command=template_command,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    (gce_instance_start >> fetch_install_code >> deploy_model >> gce_instance_stop)
