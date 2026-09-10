import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

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

## SA for Vertex Endpoints Continuous Deployment, map env names for new infra
env_short_name_mapping = {
    "prod": "prd",
    "stg": "stg",
    "dev": "dev",
}
VERTEX_ENDPOINTS_CD_SA = f"sa-vertex-endpoints-{env_short_name_mapping[ENV_SHORT_NAME]}@{GCP_PROJECT_ID}.iam.gserviceaccount.com"

RANKING_DICT = {
    "prod": "n1-highcpu-4",
    "stg": "n1-highcpu-2",
    "dev": "n1-highcpu-2",
}
CORESERVATION_RETRIEVAL_DICT = {
    "prod": "n1-standard-4",
    "stg": "n1-standard-2",
    "dev": "n1-standard-2",
}
GRAPH_RETRIEVAL_DICT = {
    "prod": "n1-standard-4",
    "stg": "n1-standard-2",
    "dev": "n1-standard-2",
}
SEMANTIC_RETRIEVAL_DICT = {
    "prod": "n1-standard-4",
    "stg": "n1-standard-2",
    "dev": "n1-standard-2",
}


@dataclass
class ModelDeployment:
    experiment_name: str
    endpoint_name: str
    instance_type: str
    min_nodes: int
    max_nodes: int
    version_name: str = "v_{{ ts_nodash }}"
    serving_env_vars: Optional[dict] = None
    service_account: Optional[str] = None


models_to_deploy = [
    # ranking endpoint
    ModelDeployment(
        experiment_name=f"ranking_endpoint_v1.1_{ENV_SHORT_NAME}",
        endpoint_name=f"recommendation_user_ranking_{ENV_SHORT_NAME}",
        instance_type=RANKING_DICT[ENV_SHORT_NAME],
        min_nodes={"prod": 1, "dev": 1, "stg": 1}[ENV_SHORT_NAME],
        max_nodes={"prod": 20, "dev": 2, "stg": 2}[ENV_SHORT_NAME],
    ),
    # two-tower retrieval endpoint
    ModelDeployment(
        experiment_name=f"retrieval_recommendation_v1.2_{ENV_SHORT_NAME}",
        endpoint_name=f"recommendation_user_retrieval_{ENV_SHORT_NAME}",
        instance_type=CORESERVATION_RETRIEVAL_DICT[ENV_SHORT_NAME],
        min_nodes={"prod": 1, "dev": 1, "stg": 1}[ENV_SHORT_NAME],
        max_nodes={"prod": 20, "dev": 2, "stg": 2}[ENV_SHORT_NAME],
    ),
    # graph retrieval endpoint
    ModelDeployment(
        experiment_name=f"graph_retrieval_recommendation_v1.1_{ENV_SHORT_NAME}",
        endpoint_name=f"recommendation_graph_retrieval_{ENV_SHORT_NAME}",
        instance_type=GRAPH_RETRIEVAL_DICT[ENV_SHORT_NAME],
        min_nodes={"prod": 1, "dev": 1, "stg": 1}[ENV_SHORT_NAME],
        max_nodes={"prod": 20, "dev": 2, "stg": 2}[ENV_SHORT_NAME],
    ),
    # semantic item retrieval endpoint
    ModelDeployment(
        experiment_name=f"semantic_item_retrieval_v1.0_{ENV_SHORT_NAME}",
        endpoint_name=f"semantic_item_retrieval_{ENV_SHORT_NAME}",
        instance_type=SEMANTIC_RETRIEVAL_DICT[ENV_SHORT_NAME],
        min_nodes={"prod": 1, "dev": 1, "stg": 1}[ENV_SHORT_NAME],
        max_nodes={"prod": 20, "dev": 2, "stg": 2}[ENV_SHORT_NAME],
        serving_env_vars={
            "SEMANTIC_LANCE_DB_URI": f"gs://{DATA_GCS_BUCKET_NAME}/semantic_search_lancedb/",
        },
        service_account=VERTEX_ENDPOINTS_CD_SA,
    ),
]


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="ML Default Deployment job",
    schedule=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
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
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.11",
        base_dir=BASE_DIR,
        retries=2,
    )

    with TaskGroup("deploy_models", dag=dag) as deploy_models:
        for model_params in models_to_deploy:
            # Single-quote the JSON so the shell passes it as one argument.
            serving_env_vars_arg = (
                f" --serving-env-vars '{json.dumps(model_params.serving_env_vars)}'"
                if model_params.serving_env_vars
                else ""
            )
            service_account_arg = (
                f" --service-account {model_params.service_account}"
                if model_params.service_account
                else ""
            )
            deploy_command = f"""
                python deploy_model.py \
                    --region {DEFAULT_REGION} \
                    --experiment-name {model_params.experiment_name} \
                    --endpoint-name {model_params.endpoint_name} \
                    --version-name {model_params.version_name} \
                    --instance-type {model_params.instance_type} \
                    --min-nodes {model_params.min_nodes} \
                    --max-nodes {model_params.max_nodes}{serving_env_vars_arg} \
                    {service_account_arg}
            """

            SSHGCEOperator(
                task_id=f"deploy_model_{model_params.experiment_name}_{model_params.endpoint_name}",
                instance_name=GCE_INSTANCE,
                base_dir=BASE_DIR,
                command=deploy_command,
                dag=dag,
            )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    gce_instance_start >> fetch_install_code >> deploy_models >> gce_instance_stop
