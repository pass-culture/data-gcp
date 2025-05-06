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
from common.utils import get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.utils.task_group import TaskGroup

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"algo-default-deployment-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/algo_training"
DAG_NAME = "algo_default_deployment"

RANKING_DICT = {
    "prod": "n1-highcpu-4",
    "stg": "n1-highcpu-2",
    "dev": "n1-highcpu-2",
}
RETRIEVAL_DICT = {
    "prod": "n1-standard-4",
    "stg": "n1-standard-2",
    "dev": "n1-standard-2",
}
SEMANTIC_DICT = {
    "prod": "n1-standard-4",
    "stg": "n1-standard-2",
    "dev": "n1-standard-2",
}


models_to_deploy = [
    # ranking endpoint
    {
        "experiment_name": f"ranking_endpoint_v1.1_{ENV_SHORT_NAME}",
        "endpoint_name": f"recommendation_user_ranking_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
        "instance_type": RANKING_DICT[ENV_SHORT_NAME],
        "min_nodes": {"prod": 1, "dev": 1, "stg": 1}[ENV_SHORT_NAME],
        "max_nodes": {"prod": 20, "dev": 2, "stg": 2}[ENV_SHORT_NAME],
    },
    # retrieval endpoint
    {
        "experiment_name": f"retrieval_recommendation_v1.2_{ENV_SHORT_NAME}",
        "endpoint_name": f"recommendation_user_retrieval_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
        "instance_type": RETRIEVAL_DICT[ENV_SHORT_NAME],
        "min_nodes": {"prod": 1, "dev": 1, "stg": 1}[ENV_SHORT_NAME],
        "max_nodes": {"prod": 20, "dev": 2, "stg": 2}[ENV_SHORT_NAME],
    },
]


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="ML Default Deployment job",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        retries=2,
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_DIR,
        retries=2,
    )

    with TaskGroup("deploy_models", dag=dag) as deploy_models:
        for model_params in models_to_deploy:
            experiment_name = model_params["experiment_name"]
            endpoint_name = model_params["endpoint_name"]
            version_name = model_params["version_name"]
            instance_type = model_params["instance_type"]
            min_nodes = model_params["min_nodes"]
            max_nodes = model_params["max_nodes"]
            deploy_command = f"""
                python deploy_model.py \
                    --region {DEFAULT_REGION} \
                    --experiment-name {experiment_name} \
                    --endpoint-name {endpoint_name} \
                    --version-name {version_name} \
                    --instance-type {instance_type} \
                    --min-nodes {min_nodes} \
                    --max-nodes {max_nodes}
            """

            SSHGCEOperator(
                task_id=f"deploy_model_{experiment_name}_{endpoint_name}",
                instance_name=GCE_INSTANCE,
                base_dir=BASE_DIR,
                command=deploy_command,
                dag=dag,
            )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    gce_instance_start >> fetch_install_code >> deploy_models >> gce_instance_stop
