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
            default=f"algo_training_version_b_{ENV_SHORT_NAME}",
            type="string",
        ),
        "endpoint_name": Param(
            default=f"recommendation_version_b_{ENV_SHORT_NAME}",
            type="string",
        ),
        "version_name": Param(
            default=f"v_YYYYMMDD",
            type="string",
        ),
        "default_region": Param(
            default=DEFAULT_REGION,
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

    template_command = r"""
        python deploy_model.py \
            --region {{ params.default_region }} \
            --experiment-name {{ params.experiment_name }} \
            --endpoint-name {{ params.endpoint_name }} \
            --version-name {{ params.version_name }}'
    """

    deploy_model = BashOperator(
        task_id=f"deploy_custom_model",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command '{DEFAULT} {template_command}'
        """,
        dag=dag,
    )

    deploy_model.set_upstream(install_dependencies)

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    gce_instance_stop.set_upstream(deploy_model)
