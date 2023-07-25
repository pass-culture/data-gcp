from airflow import DAG
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from airflow.models import Param
from datetime import datetime, timedelta
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import ENV_SHORT_NAME, DAG_FOLDER

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
BASE_DIR = "data-gcp/jobs/ml_jobs/algo_training"
gce_params = {
    "instance_name": f"sim-offers-custom-build-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-highmem-16",
        "prod": "n1-highmem-32",
    },
}


with DAG(
    "sim_offers_custom_model_build",
    default_args=default_args,
    description="Similar Offers Custom Building job",
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
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"],
            type="string",
        ),
        "experiment_name": Param(
            default=f"similar_offers_two_towers_v1.1_{ENV_SHORT_NAME}", type="string"
        ),
        "model_name": Param(default=f"v0.0_{ENV_SHORT_NAME}", type="string"),
        "run_id": Param(default="", type="string"),
        "source_experiment_name": Param(
            default=f"algo_training_two_towers_v1.1_{ENV_SHORT_NAME}", type="string"
        ),
        "source_run_id": Param(default="", type="string"),
        "source_artifact_uri": Param(default="", type="string"),
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name="{{ params.instance_name }}",
        command="{{ params.branch }}",
        python_version="3.10",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
        dag=dag,
        retries=2,
    )

    sim_offers = SSHGCEOperator(
        task_id="containerize_similar_offers",
        instance_name="{{ params.instance_name }}",
        base_dir=f"{BASE_DIR}/similar_offers",
        command="python deploy_model.py "
        "--experiment-name {{ params.experiment_name }} "
        "--model-name {{ params.model_name }} "
        "--source-experiment-name {{ params.source_experiment_name }} "
        "--source-run-id {{ params.source_run_id }} "
        "--source-artifact-uri {{  params.source_artifact_uri }} ",
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    (
        gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> sim_offers
        >> gce_instance_stop
    )
