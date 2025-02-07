from datetime import datetime, timedelta

from common import macros
from common.alerts import on_failure_combined_callback
from common.config import DAG_FOLDER, ENV_SHORT_NAME
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_combined_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
DAG_NAME = "retrieval_vector_build"

gce_params = {
    "base_dir": "data-gcp/jobs/ml_jobs/retrieval_vector",
    "instance_name": f"retrieval-recommendation-build-{ENV_SHORT_NAME}",
    "experiment_name": f"retrieval_recommendation_v1.2_{ENV_SHORT_NAME}",
    "model_name": {
        "dev": "dummy_user_recommendation",
        "stg": "two_towers_user_recommendation",
        "prod": "two_towers_user_recommendation",
    },
    "source_experiment_name": {
        "dev": f"dummy_{ENV_SHORT_NAME}",
        "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
        "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
    },
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-8",
        "prod": "n1-standard-8",
    },
    "container_worker": {"dev": "1", "stg": "1", "prod": "1"},
}

schedule_dict = {"prod": "0 4 * * *", "dev": "0 6 * * *", "stg": "0 6 * * 3"}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Custom Building job",
    schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "base_dir": Param(
            default=gce_params["base_dir"],
            type="string",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"],
            type="string",
        ),
        "experiment_name": Param(default=gce_params["experiment_name"], type="string"),
        "model_name": Param(
            default=gce_params["model_name"][ENV_SHORT_NAME], type="string"
        ),
        "container_worker": Param(
            default=gce_params["container_worker"][ENV_SHORT_NAME], type="string"
        ),
        "source_experiment_name": Param(
            default=gce_params["source_experiment_name"][ENV_SHORT_NAME], type="string"
        ),
        "source_run_id": Param(default=".", type="string"),
        "source_artifact_uri": Param(default=".", type="string"),
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir="{{ params.base_dir }}",
        retries=2,
    )

    if ENV_SHORT_NAME == "dev":
        # dummy deploy
        retrieval = SSHGCEOperator(
            task_id="containerize_retrieval_vector",
            instance_name="{{ params.instance_name }}",
            base_dir="{{ params.base_dir }}",
            command="python deploy_dummy.py "
            "--experiment-name {{ params.experiment_name }} "
            "--model-name {{ params.model_name }} ",
        )
    else:
        retrieval = SSHGCEOperator(
            task_id="containerize_retrieval_vector",
            instance_name="{{ params.instance_name }}",
            base_dir="{{ params.base_dir }}",
            command="python deploy_model.py "
            "--experiment-name {{ params.experiment_name }} "
            "--model-name {{ params.model_name }} "
            "--source-experiment-name {{ params.source_experiment_name }} "
            "--source-run-id {{ params.source_run_id }} "
            "--source-artifact-uri {{  params.source_artifact_uri }} "
            "--container-worker {{ params.container_worker }} ",
        )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    (gce_instance_start >> fetch_install_code >> retrieval >> gce_instance_stop)
