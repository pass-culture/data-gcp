from datetime import datetime
from datetime import timedelta

from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow import DAG
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
)
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)


DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "BASE_DIR": "data-gcp/jobs/playground_vm",
}

# Params
# default instance type prod :  "n1-highmem-32"
gce_params = {
    "instance_name": f"playground-vm-yourname-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-2",
        "prod": "n1-standard-2",
    },
}

default_args = {
    "start_date": datetime(2022, 11, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "launch_vm",
    default_args=default_args,
    description="Launch a vm to work on",
    schedule=None,
    catchup=False,
    dagrun_timeout=None,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"]["prod"], type="string"
        ),
        "instance_name": Param(default=gce_params["instance_name"], type="string"),
        "gpu_count": Param(default=0, type="integer"),
        "gpu_type": Param(default="nvidia-tesla-t4", type="string"),
        "keep_alive": Param(default="true", type="string"),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        labels={"keep_alive": "{{ params.keep_alive }}"},
        accelerator_types=[
            {"name": "{{ params.gpu_type }}", "count": "{{ params.gpu_count }}"}
        ],
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name="{{ params.instance_name }}",
        python_version="3.10",
        command="{{ params.branch }}",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

    (start >> gce_instance_start >> fetch_code >> install_dependencies)
