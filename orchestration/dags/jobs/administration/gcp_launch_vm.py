from datetime import datetime, timedelta
from itertools import chain

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCE_ZONES,
    INSTANCES_TYPES,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    StartGCEOperator,
)

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
DAG_DOC = """
    ### Launch VM Dag
    Use this DAG to launch a VM to work on.

    #### Parameters:
    * Working with or without a GPU :
        * if you don't need any GPU, leave the `gpu_count` parameter to 0
        * if you need a specific GPU, you might want to check the GPU availability per GCP zone [here](https://cloud.google.com/compute/docs/gpus/gpu-regions-zones)
        * For L4 GPUs, make sure to select a compatible g2 machine. The Number of L4 GPUs you can attach to a G2 depends on its RAM.
            Here is the breakdown:
                    "g2-standard-4/8/12/16/32": 1 L4,
                    "g2-standard-24": 2 L4s,
                    "g2-standard-48": 4 L4s,
                    "g2-standard-96": 8 L4s,
            ⚠️ caution: frequent stockouts on L4 GPUs, especially in europe-west1-b, try europe-west1-c or europe-west1-d if you encounter stockouts.
    * provisioning_model: leave to `STANDARD` for an immediate start. Set to `FLEX_START`
      to use Dynamic Workload Scheduler (DWS), which queues the request until capacity is
      available instead of failing on stockout — handy for GPUs that frequently stock out.
      With FLEX_START you must set `max_run_duration` (the VM is auto-deleted after it), and
      `request_valid_for_duration` controls how long the request stays queued (max 2h). The
      task does not defer while the request is queued.
      FLEX_START is incompatible with `reservation_name` and preemptible.
    * reservation_name: if you have a specific Compute Engine reservation to consume, set this parameter to the name of the reservation. When set, the VM targets this reservation via SPECIFIC_RESERVATION and requires provisioning_model=STANDARD (incompatible with FLEX_START). The instance_type, gpu_type, gpu_count and gce_zone must match the reservation exactly. Leave empty to not target any reservation.
    * use_gke_network: if you need your VM to comminicate with the Clickhouse cluster, set this parameter to True
    * pricing: the pricing of the VM depends on the `instance_type` and can be found [here](https://gcloud-compute.com/instances.html)
      ** For instance, the `n1-standard-2` instance type costs $0.1157 per hour while the `n1-standard-32` instance type costs $1.852 per hour
    """


with (
    DAG(
        "gcp_launch_vm",
        default_args=default_args,
        description="Launch a vm to work on",
        schedule=None,
        catchup=False,
        dagrun_timeout=None,
        template_searchpath=DAG_FOLDER,
        render_template_as_native_obj=True,  # be careful using this because "3.10" is rendered as 3.1 if not double escaped
        tags=[DAG_TAGS.VM.value],
        params={
            "branch": Param(
                default="production" if ENV_SHORT_NAME == "prod" else "master",
                type="string",
            ),
            "base_dir": Param(
                default="data-gcp",
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
            "gce_zone": Param(default="europe-west1-b", enum=GCE_ZONES),
            "keep_alive": Param(default=True, type="boolean"),
            "install_project": Param(default=True, type="boolean"),
            "use_gke_network": Param(default=False, type="boolean"),
            "disk_size_gb": Param(default="100", type="string"),
            "install_type": Param(
                default="simple", enum=["simple", "engineering", "science", "analytics"]
            ),
            "python_version": Param(
                default="'3.10'",
                enum=["'3.8'", "'3.9'", "'3.10'", "'3.11'", "'3.12'", "'3.13'"],
            ),
            "provisioning_model": Param(
                default="STANDARD",
                enum=["STANDARD", "FLEX_START"],
                description="""VM provisioning model. STANDARD requests capacity
                            immediately (fails on stockout). FLEX_START uses Dynamic
                            Workload Scheduler (DWS) to queue the request until
                            capacity is available (queue held for up to
                            request_valid_for_duration, max 2h). Useful for GPUs
                            with frequent stockouts.""",
            ),
            "max_run_duration": Param(
                default="12h",
                type="string",
                description="""(FLEX_START only) Max VM run duration before it is
                            auto-deleted. Accepts e.g. '12h', '1d2h', or seconds.
                            Max 7 days.""",
            ),
            "request_valid_for_duration": Param(
                default="2h",
                type="string",
                description="""(FLEX_START only) How long DWS holds the request in
                            queue while the VM is PENDING. Accepts e.g. '2h', '90m'.
                            Must be between 90s and 2h. If set to 0, the request is held for the max 2h.""",
            ),
            "reservation_name": Param(
                default="",
                type="string",
                description="""Name of a specific Compute Engine reservation to
                            consume. When set, the VM targets this reservation via
                            SPECIFIC_RESERVATION and requires provisioning_model=STANDARD
                            (incompatible with FLEX_START). The instance_type, gpu_type,
                            gpu_count and gce_zone must match the reservation exactly.
                            Leave empty to not target any reservation.""",
            ),
        },
        doc_md=DAG_DOC,
    ) as dag
):
    start = EmptyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        labels={"keep_alive": "{{ params.keep_alive|lower }}", "dag_name": "launch_vm"},
        use_gke_network="{{ params.use_gke_network }}",
        disk_size_gb="{{ params.disk_size_gb }}",
        gce_zone="{{ params.gce_zone }}",
        gpu_type="{{ params.gpu_type }}",
        gpu_count="{{ params.gpu_count }}",
        provisioning_model="{{ params.provisioning_model }}",
        max_run_duration="{{ params.max_run_duration }}",
        request_valid_for_duration="{{ params.request_valid_for_duration }}",
        reservation_name="{{ params.reservation_name }}",
        # Do not defer the task, because the deferrable mode requires to query a file in the VM started, so in the case of FLEX_START, the VM is not started and the deferrable mode might crash.
        deferrable=False,
        # Cover the max 2h DWS queue wait plus provisioning/boot margin.
        execution_timeout=timedelta(hours=3),
    )

    clone_install = InstallDependenciesOperator(
        task_id="vm_project_install",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        gce_zone="{{ params.gce_zone }}",
        python_version="{{ params.python_version }}",
        base_dir="{{ params.base_dir }}",
        requirement_file="requirements.txt",
    )
    (start >> gce_instance_start >> clone_install)
