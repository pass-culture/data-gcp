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
from common.utils import get_airflow_schedule

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"algo-default-deployment-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/algo_training"

instance_dict = {
    "prod": "n1-standard-8",
    "dev": "n1-standard-2",
    "stg": "n1-standard-4",
}
schedule_dict = {"prod": "0 22 * * 5", "dev": "0 22 * * *", "stg": "0 22 * * 3"}


models_to_deploy = [
    {
        "experiment_name": f"algo_training_clicks_v2.1_{ENV_SHORT_NAME}",
        "endpoint_name": f"recommendation_default_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
        "instance_type": instance_dict[ENV_SHORT_NAME],
    },
    {
        "experiment_name": f"similar_offers_clicks_v2.1_{ENV_SHORT_NAME}",
        "endpoint_name": f"similar_offers_default_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
        "instance_type": instance_dict[ENV_SHORT_NAME],
    },
    {
        "experiment_name": f"algo_training_two_towers_v1.1_{ENV_SHORT_NAME}",
        "endpoint_name": f"recommendation_version_b_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
        "instance_type": instance_dict[ENV_SHORT_NAME],
    },
    {
        "experiment_name": f"similar_offers_two_towers_v1.1_{ENV_SHORT_NAME}",
        "endpoint_name": f"similar_offers_version_b_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
        "instance_type": instance_dict[ENV_SHORT_NAME],
    },
]


with DAG(
    "algo_default_deployment",
    default_args=default_args,
    description="ML Default Deployment job",
    schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
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

    fetch_code.set_upstream(gce_instance_start)

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
        dag=dag,
    )

    install_dependencies.set_upstream(fetch_code)

    tasks = []
    seq_task = install_dependencies
    for model_params in models_to_deploy:
        experiment_name = model_params["experiment_name"]
        endpoint_name = model_params["endpoint_name"]
        version_name = model_params["version_name"]
        instance_type = model_params["instance_type"]
        deploy_command = f"""
            python deploy_model.py \
                --region {DEFAULT_REGION} \
                --experiment-name {experiment_name} \
                --endpoint-name {endpoint_name} \
                --version-name {version_name} \
                --instance-type {instance_type}
        """

        deploy_model = SSHGCEOperator(
            task_id=f"deploy_model_{experiment_name}",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_DIR,
            command=deploy_command,
            dag=dag,
        )

        deploy_model.set_upstream(seq_task)
        seq_task = deploy_model

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    gce_instance_stop.set_upstream(seq_task)
