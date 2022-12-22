from airflow import DAG
import os
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.models import Param
from datetime import datetime, timedelta
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT_ID,
    GCE_ZONE,
    ENV_SHORT_NAME,
    DAG_FOLDER,
)

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = os.environ.get("GCE_TRAINING_INSTANCE", "algo-training-dev")
DEFAULT = f"""cd data-gcp/algo_training
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export ENV_SHORT_NAME={ENV_SHORT_NAME}
export GCP_PROJECT_ID={GCP_PROJECT_ID}
"""
models_to_deploy = [
    {
        "experiment_name": f"algo_training_v1.1_{ENV_SHORT_NAME}",
        "endpoint_name": f"recommendation_default_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
    },
    {
        "experiment_name": f"similar_offers_v1.1_{ENV_SHORT_NAME}",
        "endpoint_name": f"similar_offers_default_{ENV_SHORT_NAME}",
        "version_name": "v_{{ ts_nodash }}",
    },
]
schedule_dict = {"prod": "0 22 * * 5", "dev": "0 22 * * *", "stg": "0 22 * * 3"}


with DAG(
    "algo_default_deployment",
    default_args=default_args,
    description="ML Default Deployment job",
    schedule_interval=schedule_dict[ENV_SHORT_NAME],
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_start_task",
    )
    gce_instance_start.set_upstream(start)

    FETCH_CODE = r'"if cd data-gcp; then git checkout master && git pull && git checkout {{ params.branch }} && git pull; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp && git checkout  {{ params.branch }} && git pull; fi"'

    fetch_code = BashOperator(
        task_id="fetch_code",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {FETCH_CODE}
        """,
        dag=dag,
    )
    fetch_code.set_upstream(gce_instance_start)

    INSTALL_DEPENDENCIES = f""" '{DEFAULT}
        pip install -r requirements.txt --user'
    """

    install_dependencies = BashOperator(
        task_id="install_dependencies",
        bash_command=f"""
            gcloud compute ssh {GCE_INSTANCE} \
            --zone {GCE_ZONE} \
            --project {GCP_PROJECT_ID} \
            --command {INSTALL_DEPENDENCIES}
            """,
        dag=dag,
    )

    install_dependencies.set_upstream(fetch_code)

    tasks = []
    for model_params in models_to_deploy:
        experiment_name = model_params["experiment_name"]
        endpoint_name = model_params["endpoint_name"]
        version_name = model_params["version_name"]
        deploy_command = f""" '{DEFAULT}
            python deploy_model.py \
                --region {DEFAULT_REGION} \
                --experiment-name {experiment_name} \
                --endpoint-name {endpoint_name} \
                --version-name {version_name}'
        """

        deploy_model = BashOperator(
            task_id=f"deploy_model_{experiment_name}",
            bash_command=f"""
            gcloud compute ssh {GCE_INSTANCE} \
            --zone {GCE_ZONE} \
            --project {GCP_PROJECT_ID} \
            --command {deploy_command}
            """,
            dag=dag,
        )

        deploy_model.set_upstream(install_dependencies)
        tasks.append(deploy_model)

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    gce_instance_stop.set_upstream(tasks)
