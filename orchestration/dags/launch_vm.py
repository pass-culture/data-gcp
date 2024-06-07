from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from common.config import DAG_FOLDER, ENV_SHORT_NAME
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "BASE_PLAYGROUND_DIR": "data-gcp/jobs/playground_vm",
    "BASE_INSTALL_DIR": "data-gcp",
    "COMMAND_INSTALL_PLAYGROUND": "pip install -r requirements.txt --user",
    "COMMAND_INSTALL_PROJECT": "NO_GCP_INIT=1 make clean_install",
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
    "dag_config": dag_config,
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
        "keep_alive": Param(default=True, type="boolean"),
        "install_project": Param(default=True, type="boolean"),
    },
) as dag:

    def select_clone_task(**kwargs):
        input_parameter = kwargs["params"].get("install_project", False)
        if input_parameter is True:
            return "clone_and_setup_with_pyenv"
        else:
            return "clone_and_setup_with_conda"

    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        labels={"keep_alive": "{{ params.keep_alive|lower }}"},
        gpu_count="{{ params.gpu_count }}",
        accelerator_types=[
            {"name": "{{ params.gpu_type }}", "count": "{{ params.gpu_count }}"}
        ],
    )

    branching_clone_task = BranchPythonOperator(
        task_id="branching_clone_task",
        python_callable=select_clone_task,
        provide_context=True,
    )

    clone_and_setup_with_pyenv = CloneRepositoryGCEOperator(
        task_id="clone_and_setup_with_pyenv",
        instance_name="{{ params.instance_name }}",
        python_version="3.10",
        use_pyenv=True,
        command="{{ params.branch }}",
        retries=2,
    )

    clone_and_setup_with_conda = CloneRepositoryGCEOperator(
        task_id="clone_and_setup_with_conda",
        instance_name="{{ params.instance_name }}",
        python_version="3.10",
        use_pyenv=False,
        command="{{ params.branch }}",
        retries=2,
    )

    install_project_with_pyenv = SSHGCEOperator(
        task_id="install_project_with_pyenv",
        instance_name="{{ params.instance_name }}",
        use_pyenv=True,
        base_dir=dag_config["BASE_INSTALL_DIR"],
        command=dag_config["COMMAND_INSTALL_PROJECT"],
        dag=dag,
        retries=2,
    )

    install_playground_with_conda = SSHGCEOperator(
        task_id="install_playground_with_conda",
        instance_name="{{ params.instance_name }}",
        use_pyenv=False,
        base_dir=dag_config["BASE_PLAYGROUND_DIR"],
        command=dag_config["COMMAND_INSTALL_PLAYGROUND"],
        dag=dag,
        retries=2,
    )

    clone_and_setup_with_pyenv >> install_project_with_pyenv
    clone_and_setup_with_conda >> install_playground_with_conda
    (
        start
        >> gce_instance_start
        >> branching_clone_task
        >> [clone_and_setup_with_pyenv, clone_and_setup_with_conda]
    )
