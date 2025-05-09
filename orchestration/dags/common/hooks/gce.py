import datetime
import json
import time
import typing as t

import dateutil
import googleapiclient.discovery
import pytz
from common.config import (
    ENV_SHORT_NAME,
    GCE_SA,
    GCE_ZONE,
    GCP_PROJECT_ID,
    STOP_UPON_FAILURE_LABELS,
)
from common.hooks.image import CPUImage
from common.hooks.network import BASE_NETWORK_LIST, VPCNetwork
from googleapiclient.errors import HttpError

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.context import Context

DEFAULT_LABELS = {
    "env": ENV_SHORT_NAME,
    "terraform": "false",
    "airflow": "true",
    "keep_alive": "false",
    "job_type": "default",
}


class DeferrableSSHGCEJobManager:
    """Handles remote job management operations"""

    def __init__(self, task_id: str, run_id: str):
        self.task_id = task_id
        self.run_id = run_id
        self.job_id = f"{task_id}_{run_id}"

    @property
    def job_base_dir(self) -> str:
        return "~/airflow_jobs"

    @property
    def job_dir(self) -> str:
        return f"{self.job_base_dir}/{self.job_id}"

    @property
    def setup_script(self) -> str:
        return f"""
            # Ensure base directory exists
            mkdir -p {self.job_base_dir}

            # Create job-specific directory
            JOB_DIR={self.job_dir}
            mkdir -p $JOB_DIR

            # Initialize job status
            echo "running" > $JOB_DIR/status
        """

    @property
    def trap_handlers(self) -> str:
        return f"""
            trap 'echo "failed" > {self.job_dir}/status; exit 1' ERR
            trap 'echo "interrupted" > {self.job_dir}/status; exit 1' INT TERM
        """

    def wrap_command(self, command: str) -> str:
        return f"""
            {self.setup_script}

            (
                {self.trap_handlers}

                # Execute the command and capture output
                {command} > {self.job_dir}/output 2>&1

                # Mark successful completion
                echo "completed" > {self.job_dir}/status
            ) </dev/null >/dev/null 2>&1 &

            # Store the background job's PID
            echo $! > {self.job_dir}/pid

            # Return the job ID for monitoring
            echo "{self.job_id}"
            exit 0
        """

    def get_status_check_command(self) -> str:
        return f"""
            JOB_DIR={self.job_dir}
            if [ -f "$JOB_DIR/status" ]; then
                status=$(cat $JOB_DIR/status)
                pid=$(cat $JOB_DIR/pid 2>/dev/null || echo "0")

                # Check if process is still running
                if [ "$status" = "running" ] && ! kill -0 $pid 2>/dev/null; then
                    echo "failed" > $JOB_DIR/status
                    echo "Process died unexpectedly" >> $JOB_DIR/output
                fi

                output=$(cat $JOB_DIR/output 2>/dev/null || echo "No output yet")
                echo "STATUS:$(cat $JOB_DIR/status)"
                echo "OUTPUT:$output"
                echo "PID:$pid"
            else
                echo "STATUS:failed"
                echo "OUTPUT:Job directory not found"
                echo "PID:0"
            fi
        """

    def get_cleanup_command(self) -> str:
        """Returns command to clean up job directory after completion or failure."""
        return f"""
            # Only remove if job is not running
            JOB_DIR={self.job_dir}
            if [ -f "$JOB_DIR/status" ]; then
                status=$(cat $JOB_DIR/status)
                if [ "$status" != "running" ]; then
                    # Archive the output before cleaning
                    mkdir -p {self.job_base_dir}/archive
                    cp $JOB_DIR/output {self.job_base_dir}/archive/{self.job_id}_output.log 2>/dev/null || true
                    rm -rf $JOB_DIR
                fi
            fi
        """


class GCEHook(GoogleBaseHook):
    _conn = None

    def __init__(
        self,
        gcp_project: str = GCP_PROJECT_ID,
        gce_zone: str = GCE_ZONE,
        gce_networks: t.List[VPCNetwork] = BASE_NETWORK_LIST,
        gce_sa: str = GCE_SA,
        source_image_type: CPUImage = CPUImage(),
        gcp_conn_id: str = "google_cloud_default",
        disk_size_gb: str = "100",
        impersonation_chain: str = None,
    ):
        self.gcp_project = gcp_project
        self.gce_zone = gce_zone
        self.gce_networks = gce_networks
        self.gce_sa = gce_sa
        self.disk_size_gb = disk_size_gb
        self.source_image_type = source_image_type
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self):
        if self._conn is None:
            self._conn = googleapiclient.discovery.build(
                "compute",
                "v1",
                cache_discovery=False,
            )
        return self._conn

    def start_vm(
        self,
        instance_name: str,
        instance_type: str,
        preemptible,
        labels={},
        gpu_count: int = 0,
        gpu_type: t.Optional[str] = None,
    ):
        instances = self.list_instances()
        instances = [x["name"] for x in instances if x["status"] == "RUNNING"]
        if instance_name in instances:
            self.log.info(f"Instance {instance_name} already running, pass.")
            return

        self.log.info(
            f"Launching {instance_name} on compute engine (instance: {instance_type})"
        )
        self.__create_instance(
            instance_type,
            instance_name,
            labels=labels,
            wait=True,
            preemptible=preemptible,
            gpu_type=gpu_type,
            gpu_count=gpu_count,
        )

    def delete_vm(self, instance_name):
        self.log.info(f"Deleting {instance_name} on compute engine")
        self.__delete_instance(instance_name, wait=True)

    def list_instances(self, filter=None):
        result = (
            self.get_conn()
            .instances()
            .list(project=self.gcp_project, zone=self.gce_zone, filter=filter)
            .execute()
        )
        return result.get("items", [])

    def get_instance(self, name):
        try:
            return (
                self.get_conn()
                .instances()
                .get(instance=name, project=self.gcp_project, zone=self.gce_zone)
                .execute()
            )
        except HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise

    def __create_instance(
        self,
        instance_type,
        name,
        labels,
        metadata=None,
        wait=False,
        gpu_count: int = 0,
        gpu_type: t.Optional[str] = None,
        preemptible=False,
    ):
        instance_type = "zones/%s/machineTypes/%s" % (self.gce_zone, instance_type)
        metadata = (
            [{"key": key, "value": value} for key, value in metadata.items()]
            if metadata
            else []
        )
        if self.source_image_type.startup_script is not None:
            metadata = metadata + [
                {
                    "key": "startup-script",
                    "value": self.source_image_type.startup_script,
                }
            ]

        config = {
            "name": name,
            "machineType": instance_type,
            # Specify the boot disk and the image to use as a source.
            "disks": [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initialize_params": {
                        "disk_size_gb": self.disk_size_gb,
                        "sourceImage": self.source_image_type.source_image,
                    },
                }
            ],
            # Specify VPC network interface
            "networkInterfaces": [
                {
                    "network": network.network_id,
                    "subnetwork": network.subnetwork_id,
                }
                for network in self.gce_networks
            ],
            "serviceAccounts": [
                {
                    "email": f"{self.gce_sa}@{self.gcp_project}.iam.gserviceaccount.com",
                    "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                }
            ],
            "metadata": {"items": metadata},
            "tags": {"items": ["training"]},
            "labels": dict({**DEFAULT_LABELS, **labels}),
        }

        # GPUs
        if gpu_count > 0:
            config["guestAccelerators"] = [
                {
                    "acceleratorCount": gpu_count,
                    "acceleratorType": f"zones/{self.gce_zone}/acceleratorTypes/{gpu_type}",
                }
            ]
        if preemptible:
            config["scheduling"] = {
                "onHostMaintenance": "terminate",
                "preemptible": True,
            }
        else:
            config["scheduling"] = {"onHostMaintenance": "terminate"}

        self.log.info(
            f"Creating {name}: \n {json.dumps(config, sort_keys=True, indent=4)}"
        )
        operation = (
            self.get_conn()
            .instances()
            .insert(project=self.gcp_project, zone=self.gce_zone, body=config)
            .execute()
        )

        if wait:
            # force some waiting time for startup script.
            if self.source_image_type.startup_script_wait_time > 0:
                time.sleep(self.source_image_type.startup_script_wait_time)
            self.wait_for_operation(operation["name"])
        else:
            return operation["name"]

    def __delete_instance(self, name, wait=False):
        self.log.info(f"Deleting {name}")
        try:
            operation = (
                self.get_conn()
                .instances()
                .delete(project=self.gcp_project, zone=self.gce_zone, instance=name)
                .execute()
            )
            if wait:
                self.wait_for_operation(operation["name"])
            else:
                return operation["name"]
        except HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise

    def __stop_instance(self, name, wait=False):
        self.log.info(f"Stopping {name}")
        try:
            operation = (
                self.get_conn()
                .instances()
                .stop(project=self.gcp_project, zone=self.gce_zone, instance=name)
                .execute()
            )
            if wait:
                self.wait_for_operation(operation["name"])
            else:
                return operation["name"]
        except HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise

    def stop_vm(self, instance_name):
        self.log.info(f"Stopping {instance_name} on compute engine")
        self.__stop_instance(instance_name, wait=True)

    def delete_instances(self, job_type="default", timeout_in_minutes=60 * 12):
        instances = self.list_instances()

        instances = [
            x
            for x in instances
            if x.get("labels", {}).get("airflow", "") == "true"
            and x.get("labels", {}).get("env", "") == ENV_SHORT_NAME
            and not x.get("labels", {}).get("keep_alive", "false") == "true"
            and x.get("labels", {}).get("job_type", "default") == job_type
        ]

        for instance in instances:
            creation = dateutil.parser.parse(instance["creationTimestamp"])
            now = datetime.datetime.now(pytz.utc)
            instance_life_minutes = (now - creation) / datetime.timedelta(minutes=1)
            if instance_life_minutes > timeout_in_minutes:
                self.__delete_instance(instance["name"])

    def wait_for_operation(self, operation):
        self.log.info(f"Waiting for operation {operation} to finish ...")
        retry = False
        while True:
            try:
                result = (
                    self.get_conn()
                    .zoneOperations()
                    .get(
                        project=self.gcp_project,
                        zone=self.gce_zone,
                        operation=operation,
                    )
                    .execute()
                )
                if result["status"] == "DONE":
                    if "error" in result:
                        raise Exception(result["error"])
                    return result

                time.sleep(60)
            except Exception as e:
                if not retry:
                    self.log.info("Got except... sleep...")
                    time.sleep(60 * 5)
                    retry = True
                else:
                    raise e


def on_failure_callback_stop_vm(context: Context):
    """
    This callback stops the VM associated with the failing task,
    assuming the failing task has an `instance_name` attribute.
    """
    failing_task = context.get("task")
    if hasattr(failing_task, "instance_name"):
        # Ensure any templated fields are rendered
        failing_task.render_template_fields(context)
        instance_name = failing_task.instance_name

        failing_task.log.info(f"Stopping VM {instance_name} due to task failure.")

        hook = GCEHook()
        instance_details = hook.get_instance(instance_name)
        if instance_details:
            labels = instance_details.get("labels", {})
            failing_task.log.info(f"Retrieved labels for {instance_name}: {labels}")
            if labels.get("job_type") in STOP_UPON_FAILURE_LABELS:
                failing_task.log.info(
                    f"Stopping VM '{instance_name}' because label 'job_type' in {STOP_UPON_FAILURE_LABELS}."
                )
                hook.stop_vm(instance_name)
            else:
                failing_task.log.info(
                    f"Not stopping VM '{instance_name}'; label 'job_type' is not set to 'long_ml'."
                    f" Current labels: {labels}"
                )
        else:
            failing_task.log.info(
                f"Instance '{instance_name}' not found; cannot perform VM stop."
            )
