from datetime import datetime, timedelta
from itertools import chain

from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCE_ZONES,
    INSTANCES_TYPES,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    StartGCEOperator,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "BASE_PLAYGROUND_DIR": "data-gcp/jobs/playground_vm",
    "BASE_INSTALL_DIR": "data-gcp",
    "COMMAND_INSTALL_PLAYGROUND": "pip install -r requirements.txt --user",
    "COMMAND_INSTALL_PROJECT": "NO_GCP_INIT=1 make install",
    "PREFIX_COMMAND_INSTALL_PROJECT_UV": "NO_GCP_INIT=1 make ",
}

# Params
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
dag_doc = doc_md = """
    ### Launch VM Dag
    Use this DAG to launch a VM to work on.

    #### Parameters:
    * Working with or without a GPU :
        * if you don't need any GPU, leave the `gpu_count` parameter to 0
        * if you need a specific GPU, you might want to check the GPU availability per GCP zone [here](https://cloud.google.com/compute/docs/gpus/gpu-regions-zones)
    * use_gke_network: if you need your VM to comminicate with the Clickhouse cluster, set this parameter to True
    * pricing: the pricing of the VM depends on the `instance_type` and can be found [here](https://gcloud-compute.com/instances.html)
      ** For instance, the `n1-standard-2` instance type costs $0.1157 per hour while the `n1-standard-32` instance type costs $1.852 per hour
    """


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
        "gce_zone": Param(default="europe-west-1b", enum=GCE_ZONES),
        "keep_alive": Param(default=True, type="boolean"),
        "install_project": Param(default=True, type="boolean"),
        "use_gke_network": Param(default=False, type="boolean"),
        "disk_size_gb": Param(default="100", type="string"),
        "installer": Param(default="uv", enum=["uv", "conda"]),
        "install_type": Param(
            default="simple", enum=["simple", "engineering", "science", "analytics"]
        ),
        "python_version": Param(
            default="'3.10'",
            enum=["'3.8'", "'3.9'", "'3.10'", "'3.11'", "'3.12'", "'3.13'"],
        ),
    },
    doc_md=dag_doc,
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        labels={"keep_alive": "{{ params.keep_alive|lower }}"},
        use_gke_network="{{ params.use_gke_network }}",
        disk_size_gb="{{ params.disk_size_gb }}",
        gce_zone="{{ params.gce_zone }}",
        gpu_type="{{ params.gpu_type }}",
        gpu_count="{{ params.gpu_count }}",
    )

    clone_install = InstallDependenciesOperator(
        task_id="vm_project_install",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        installer="{{ params.installer }}",
        gce_zone="{{ params.gce_zone }}",
        python_version="{{ params.python_version }}",
        requirement_file="requirements.txt",
    )

    (start >> gce_instance_start >> clone_install)
