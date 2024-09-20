from datetime import datetime, timedelta
from itertools import chain

from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    INSTALL_TYPES,
    INSTANCES_TYPES,
)
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "BASE_PLAYGROUND_DIR": "data-gcp/jobs/playground_vm",
    "BASE_INSTALL_DIR": "data-gcp",
    "COMMAND_INSTALL_PLAYGROUND": "pip install -r requirements.txt --user",
    "COMMAND_INSTALL_PROJECT": "NO_GCP_INIT=1 make install",
    "COMMAND_INSTALL_PROJECT_UV": "NO_GCP_INIT=1 make ",
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
    render_template_as_native_obj=True,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"]["prod"],
            enum=list(chain(*INSTANCES_TYPES["cpu"].values())),
        ),
        "instance_name": Param(default=gce_params["instance_name"], type="string"),
        "gpu_count": Param(default=0, enum=INSTANCES_TYPES["gpu"]["count"]),
        "gpu_type": Param(
            default="nvidia-tesla-t4", enum=INSTANCES_TYPES["gpu"]["name"]
        ),
        "keep_alive": Param(default=True, type="boolean"),
        "install_project": Param(default=True, type="boolean"),
        "use_gke_network": Param(default=False, type="boolean"),
        "disk_size_gb": Param(default="100", type="string"),
        "install_with_uv": Param(default=False, type="boolean"),
        "install_type": Param(
            default="simple", enum=["simple", "engineering", "science", "analytics"]
        ),
    },
) as dag:

    def select_clone_task(**kwargs):
        input_parameter = kwargs["params"].get("install_project", False)
        install_with_uv = kwargs["params"].get("install_with_uv", False)
        if install_with_uv:
            return "clone_and_setup_with_uv"
        else:
            if input_parameter:
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
        use_gke_network="{{ params.use_gke_network }}",
        disk_size_gb="{{ params.disk_size_gb }}",
    )

    branching_clone_task = BranchPythonOperator(
        task_id="branching_clone_task",
        python_callable=select_clone_task,
        provide_context=True,
    )

    clone_and_setup_with_uv = CloneRepositoryGCEOperator(
        task_id="clone_and_setup_with_uv",
        instance_name="{{ params.instance_name }}",
        python_version="3.10",
        use_uv=True,
        command="{{ params.branch }}",
        retries=2,
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

    install_project_with_uv = SSHGCEOperator(
        task_id="install_project_with_uv",
        instance_name="{{ params.instance_name }}",
        use_pyenv=True,
        base_dir=dag_config["BASE_INSTALL_DIR"],
        command=dag_config["COMMAND_INSTALL_PROJECT_UV"]
        + INSTALL_TYPES[dag.params["install_type"]],
        dag=dag,
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

    def get_op_by_name(name, step):
        assert name in ["uv", "pyenv", "conda"]
        assert step in ["clone", "install"]
        if step == "clone":
            return {
                "uv": clone_and_setup_with_uv,
                "pyenv": clone_and_setup_with_pyenv,
                "conda": clone_and_setup_with_conda,
            }[name]
        else:
            return {
                "uv": install_project_with_uv,
                "pyenv": install_project_with_pyenv,
                "conda": install_playground_with_conda,
            }[name]

    (start >> gce_instance_start >> branching_clone_task)
    for pkg_manager in ["uv", "pyenv", "conda"]:
        (
            branching_clone_task
            >> get_op_by_name(pkg_manager, "clone")
            >> get_op_by_name(pkg_manager, "install")
        )
