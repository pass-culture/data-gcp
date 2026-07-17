import asyncio
import datetime
import json
import os
import re
import time
import typing as t
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import sleep

import dateutil
import googleapiclient.discovery
import pytz
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.context import Context
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
from paramiko import SSHException

DEFAULT_LABELS = {
    "env": ENV_SHORT_NAME,
    "terraform": "false",
    "airflow": "true",
    "keep_alive": "false",
    "job_type": "default",
}


@dataclass
class DeferrableSSHJobStatus:
    status: str
    output: str
    pid: str
    logs: str


class SSHGCEJobManager:
    MAX_RETRY = 3
    SSH_TIMEOUT = 10

    def __init__(
        self,
        task_id: str,
        task_instance: t.Optional[TaskInstance],
        hook: ComputeEngineSSHHook,
        environment: t.Dict[str, str],
        do_xcom_push: bool = True,
        logger=None,
    ):
        self.ssh_hook = hook
        self.task_id = task_id
        self.environment = environment
        self.task_instance = task_instance
        self.do_xcom_push = do_xcom_push
        self.log = logger or (task_instance.log if task_instance else None)

    def run_ssh_client_command(self, command: str, retry=1) -> str:
        try:
            with self.ssh_hook.get_conn() as ssh_client:
                exit_status, agg_stdout, agg_stderr = (
                    self.ssh_hook.exec_ssh_client_command(
                        ssh_client,
                        command,
                        timeout=3600,
                        environment=self.environment,
                        get_pty=False,
                    )
                )
                if self.do_xcom_push and self.task_instance:
                    self.task_instance.xcom_push(key="ssh_exit", value=exit_status)
                    # Ensure there are enough lines in the output
                    lines_result = agg_stdout.decode("utf-8").split("\n")
                    if len(lines_result) == 0:
                        result = ""  # No output available
                    elif len(lines_result) == 1:
                        result = lines_result[0]  # Only one line exists, use it
                    else:
                        # Use the last or second-to-last line depending on content
                        if len(lines_result[-1]) > 0:
                            result = lines_result[-1]
                        else:
                            result = lines_result[-2]

                    # Push result to XCom
                    self.task_instance.xcom_push(key="result", value=result)

                if exit_status != 0:
                    raise AirflowException(
                        f"SSH operator error: exit status = {exit_status}"
                    )

                return self._decode_result(agg_stdout)
        except SSHException as e:
            self.log.info(
                f"Cannot connect to instance {self.instance_name}. Retry : {retry}."
            )
            if retry > self.MAX_RETRY:
                self.log.info(
                    f"Could not connect to instance {self.instance_name}. After {retry} retries. Abort."
                )
                raise e
            sleep(retry * self.SSH_TIMEOUT)
            return self.run_ssh_client_command(command, retry=retry + 1)

    def _decode_result(self, result: bytes | str) -> str:
        enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
        if not enable_pickling:
            if result is not None:
                result = b64encode(result).decode("utf-8")
        if isinstance(result, bytes):
            return result.decode("utf-8")
        return str(result)


class DeferrableSSHGCEJobManager(SSHGCEJobManager):
    """Handles remote job management operations"""

    def __init__(
        self,
        task_id: str,
        task_instance: t.Optional[TaskInstance],
        hook: ComputeEngineSSHHook,
        environment: t.Dict[str, str],
        run_id: str,
        do_xcom_push: bool = True,
        logger=None,
        *args,
        **kwargs,
    ):
        self.run_id = run_id
        self.job_id = f"{task_id}_{str(run_id)}"
        self._ssh_executor = None  # Instance-level executor, not class-level
        super().__init__(
            task_id,
            task_instance,
            hook,
            environment,
            do_xcom_push,
            logger=logger,
            *args,
            **kwargs,
        )

    @property
    def _job_base_dir(self) -> str:
        return "~/airflow_jobs"

    @property
    def _job_dir(self) -> str:
        return f"{self._job_base_dir}/{self.job_id}"

    @property
    def _setup_script(self) -> str:
        return f"""
            # Ensure base directory exists
            mkdir -p {self._job_base_dir}

            # Create job-specific directory
            JOB_DIR={self._job_dir}
            mkdir -p $JOB_DIR

            # Initialize job status
            echo "running" > $JOB_DIR/status
            sync
        """

    @property
    def _trap_handlers(self) -> str:
        return f"""
            trap 'echo "failed" > {self._job_dir}/status; exit 1' ERR
            trap 'echo "interrupted" > {self._job_dir}/status; exit 1' INT TERM
        """

    def _deferrable_command(self, command: str) -> str:
        return f"""
            {self._setup_script}

            (
                {self._trap_handlers}

                # Execute the command and capture output
                {command} > {self._job_dir}/output 2>&1

                # Mark successful completion
                echo "completed" > {self._job_dir}/status
            ) </dev/null >/dev/null 2>&1 &

            # Store the background job's PID
            echo $! > {self._job_dir}/pid

            # Return the job ID for monitoring
            echo "{self.job_id}"
            exit 0
        """

    def run_ssh_client_command(self, command: str, retry=1) -> str:
        command = self._deferrable_command(command)
        return super().run_ssh_client_command(command, retry)

    async def run_ssh_status_check_command(self, retry=1) -> DeferrableSSHJobStatus:
        command = f"""
            JOB_DIR={self._job_dir}
            ARCHIVE_LOG={self._job_base_dir}/archive/{self.job_id}_output.log
            if [ -f "$JOB_DIR/status" ]; then
                status=$(cat $JOB_DIR/status)
                pid=$(cat $JOB_DIR/pid 2>/dev/null || echo "0")

                # Check if process is still running
                if [ "$status" = "running" ] && ! kill -0 $pid 2>/dev/null; then
                    echo "failed" > $JOB_DIR/status
                    echo "Process died unexpectedly" >> $JOB_DIR/output
                fi

                # Only tail the last 1000 lines to prevent memory bloat
                output=$(tail -n 1000 $JOB_DIR/output 2>/dev/null || echo "No output yet")
                echo "STATUS:$(cat $JOB_DIR/status)"
                echo "OUTPUT:$output"
                echo "PID:$pid"
            elif [ -f "$ARCHIVE_LOG" ]; then
                # Job directory was already cleaned up after a successful run.
                # This happens when the triggerer restarts after cleanup but before
                # the TriggerEvent was delivered to the Airflow scheduler.
                output=$(tail -n 1000 "$ARCHIVE_LOG" 2>/dev/null || echo "No archived output")
                echo "STATUS:completed"
                echo "OUTPUT:$output"
                echo "PID:0"
            else
                echo "STATUS:failed"
                echo "OUTPUT:Job directory not found"
                echo "PID:0"
            fi
        """
        self.log.info("Running status check command")
        # Run sync SSH command in executor to avoid blocking event loop
        import asyncio
        import os
        from concurrent.futures import ThreadPoolExecutor

        # Create instance-level thread pool (not class-level) to prevent memory leak
        # Default: 8 threads (good for 1 CPU core with I/O-bound work)
        if self._ssh_executor is None:
            max_workers = int(os.environ.get("TRIGGERER_SSH_THREADS", "8"))
            self._ssh_executor = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="ssh_check"
            )

        loop = asyncio.get_event_loop()
        agg_output = await loop.run_in_executor(
            self._ssh_executor, super().run_ssh_client_command, command, retry
        )
        return self._parse_check_command_output(agg_output)

    async def run_ssh_cleanup_command(self, retry=1) -> str:
        """Returns command to clean up job directory after completion or failure."""
        command = f"""
            # Only remove if job is not running
            JOB_DIR={self._job_dir}
            if [ -f "$JOB_DIR/status" ]; then
                status=$(cat $JOB_DIR/status)
                if [ "$status" != "running" ]; then
                    # Archive the output before cleaning
                    mkdir -p {self._job_base_dir}/archive
                    cp $JOB_DIR/output {self._job_base_dir}/archive/{self.job_id}_output.log 2>/dev/null || true
                    rm -rf $JOB_DIR
                fi
            fi
        """
        self.log.info("Running cleanup command")
        # Run sync SSH command in executor to avoid blocking event loop

        # Reuse the instance-level thread pool
        if self._ssh_executor is None:
            max_workers = int(os.environ.get("TRIGGERER_SSH_THREADS", "8"))
            self._ssh_executor = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="ssh_check"
            )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._ssh_executor, super().run_ssh_client_command, command, retry
        )

    def __del__(self):
        """Cleanup thread pool on instance destruction to prevent memory leak."""
        if hasattr(self, "_ssh_executor") and self._ssh_executor is not None:
            self._ssh_executor.shutdown(wait=False)

    def _parse_check_command_output(self, raw: str | bytes) -> DeferrableSSHJobStatus:
        """
        Extracts STATUS and OUTPUT from raw SSH output.
        Returns (status_str or None, output_text).
        """
        # Always ensure we're working with a string
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        elif not isinstance(raw, str):
            raw = str(raw)

        m_status = re.search(r"STATUS:(\w+)", raw)
        m_output = re.search(r"OUTPUT:(.*)", raw, re.DOTALL)
        m_pid = re.search(r"PID:(\d+)", raw)
        status = m_status.group(1).lower() if m_status else None
        output = m_output.group(1).strip() if m_output else "No output available"
        pid = m_pid.group(1) if m_pid else None
        return DeferrableSSHJobStatus(status=status, output=output, pid=pid, logs=raw)


class GCEHook(GoogleBaseHook):
    _conn = None

    # A VM that reaches one of these will never become RUNNING on its own again.
    TERMINAL_STATES = frozenset(
        {"TERMINATED", "SUSPENDED", "SUSPENDING", "STOPPING", "STOPPED"}
    )

    # General margin for VM provisioning and driver installation across all models.
    # 15 minutes allows a comfortable buffer without delaying failure detection.
    BOOT_MARGIN_SECONDS = 900

    # Fallback matching GCP's maximum 2-hour queue limit for Dynamic Workload
    # Scheduling (DWS). Only applies to FLEX_START provisioning models.
    FLEX_START_MAX_QUEUE_SECONDS = 7200

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
        additional_scopes: t.List[str] = None,
    ):
        self.gcp_project = gcp_project
        self.gce_zone = gce_zone
        self.gce_networks = gce_networks
        self.gce_sa = gce_sa
        self.disk_size_gb = disk_size_gb
        self.source_image_type = source_image_type
        self.additional_scopes = additional_scopes or []
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

    def close(self):
        """Close the connection and clear resources."""
        if self._conn is not None:
            # Google API client doesn't have explicit close, but we can clear
            # the reference to allow garbage collection of connection pools
            self._conn = None

    def __del__(self):
        """Cleanup on destruction."""
        self.close()

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup on context exit."""
        self.close()
        return False  # Don't suppress exceptions

    def start_vm(
        self,
        instance_name: str,
        instance_type: str,
        preemptible,
        labels={},
        gpu_count: int = 0,
        gpu_type: t.Optional[str] = None,
        provisioning_model: str = "STANDARD",
        max_run_duration_seconds: t.Optional[int] = None,
        request_valid_for_duration_seconds: t.Optional[int] = None,
        reservation_name: t.Optional[str] = None,
        wait_for_running: bool = True,
    ) -> bool:
        """Start (or no-op on) a VM. Returns True if the instance was already RUNNING."""
        is_flex_start = (provisioning_model or "STANDARD").upper() == "FLEX_START"
        # The DWS queue-cap term only applies to flex-start; STANDARD/reservation
        # instances only need the general boot margin.
        wait_timeout = self.BOOT_MARGIN_SECONDS
        if is_flex_start:
            wait_timeout += (
                request_valid_for_duration_seconds or self.FLEX_START_MAX_QUEUE_SECONDS
            )

        # Check all statuses to prevent a retried task (e.g., after an Airflow
        # triggerer crash) from attempting a duplicate insert and hitting a 409 conflict.
        existing = self.get_instance(instance_name)
        if existing is not None:
            status = existing.get("status")
            if status == "RUNNING":
                self.log.info(f"Instance {instance_name} already running, pass.")
                return True
            if status not in self.TERMINAL_STATES:
                # Warn if an existing instance is found mid-flight. While expected on task
                # retry, it could signal a naming collision/race condition with another
                # run, which is particularly suspicious for STANDARD provisioning.
                self.log.warning(...)
                self.log.warning(
                    f"Instance {instance_name} already exists with status "
                    f"{status} (a prior insert is still in flight) instead of "
                    "being created fresh by this call. Adopting it and waiting "
                    "for it to reach RUNNING instead of re-inserting. If this "
                    f"is unexpected (provisioning_model={provisioning_model}), "
                    "check whether another task or DAG run is targeting the "
                    "same instance_name concurrently."
                )
                if wait_for_running:
                    self.wait_for_instance_running(
                        instance_name, timeout_seconds=wait_timeout
                    )
                return False

            # Clear any stale, terminally-failed instance from a previous run
            # to allow a clean, fresh insertion on retry.
            self.log.warning(
                f"Instance {instance_name} exists in terminal state {status}; "
                "deleting before retrying."
            )
            self.__delete_instance(instance_name, wait=True)

        self.log.info(
            f"Launching {instance_name} on compute engine (instance: {instance_type}, "
            f"provisioning_model: {provisioning_model})"
        )

        # Wait for STANDARD/preemptible insertions synchronously. FLEX_START
        # inserts are async queue placements, so we submit immediately and poll.
        self.__create_instance(
            instance_type,
            instance_name,
            labels=labels,
            wait=not is_flex_start,
            preemptible=preemptible,
            gpu_type=gpu_type,
            gpu_count=gpu_count,
            provisioning_model=provisioning_model,
            max_run_duration_seconds=max_run_duration_seconds,
            request_valid_for_duration_seconds=request_valid_for_duration_seconds,
            reservation_name=reservation_name,
        )
        if is_flex_start and wait_for_running:
            # Cover the max queue wait plus provisioning/boot margin, mirroring
            # the deadline the deferrable path gives GCEInstanceRunningTrigger,
            # so this synchronous wait doesn't loop forever on a stuck instance.
            self.wait_for_instance_running(instance_name, timeout_seconds=wait_timeout)
        return False

    def wait_for_instance_running(
        self,
        instance_name: str,
        timeout_seconds: t.Optional[int] = None,
        poll_interval: int = 30,
    ):
        """Poll a (flex-start) instance until it reaches RUNNING.

        DWS holds flex-start requests in a queue while the VM is PENDING. If the
        request expires without securing capacity the instance is deleted (404) or
        moves to a terminal state; both are treated as failures.
        """
        deadline = time.time() + timeout_seconds if timeout_seconds else None
        while True:
            instance = self.get_instance(instance_name)
            if instance is None:
                raise AirflowException(
                    f"Instance {instance_name} no longer exists: the flex-start "
                    "request likely expired without securing capacity."
                )
            status = instance.get("status")
            self.log.info(f"Instance {instance_name} status: {status}")
            if status == "RUNNING":
                # Give the startup script (e.g. GPU drivers) time to run, mirroring
                # the synchronous insert path.
                if self.source_image_type.startup_script_wait_time > 0:
                    time.sleep(self.source_image_type.startup_script_wait_time)
                return
            if status in self.TERMINAL_STATES:
                raise AirflowException(
                    f"Instance {instance_name} reached terminal state {status} "
                    "before RUNNING; flex-start failed to secure capacity."
                )
            if deadline and time.time() > deadline:
                raise AirflowException(
                    f"Timed out waiting for {instance_name} to reach RUNNING "
                    f"(last status: {status})."
                )
            time.sleep(poll_interval)

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

    @staticmethod
    def _apply_provisioning_config(
        config: dict,
        provisioning_model: str,
        preemptible: bool,
        max_run_duration_seconds: t.Optional[int],
        request_valid_for_duration_seconds: t.Optional[int],
        reservation_name: t.Optional[str] = None,
    ) -> None:
        """Set the scheduling (and, for flex-start, params/reservation) block."""
        provisioning_model = (provisioning_model or "STANDARD").upper()

        # Target a specific reservation using STANDARD provisioning where capacity is
        # pre-secured. This is mutually exclusive with FLEX_START queueing.
        if reservation_name:
            if provisioning_model == "FLEX_START":
                raise AirflowException(
                    "reservation_name is incompatible with FLEX_START provisioning; "
                    "set provisioning_model=STANDARD to consume a reservation."
                )
            config["scheduling"] = {"onHostMaintenance": "terminate"}
            if preemptible:
                config["scheduling"]["preemptible"] = True
            config["reservationAffinity"] = {
                "consumeReservationType": "SPECIFIC_RESERVATION",
                "key": "compute.googleapis.com/reservation-name",
                "values": [reservation_name],
            }
            return

        if provisioning_model != "FLEX_START":
            config["scheduling"] = {"onHostMaintenance": "terminate"}
            if preemptible:
                config["scheduling"]["preemptible"] = True
            return

        # FLEX_START queues the request via DWS until capacity is available.
        # It requires a max run duration and consumes preemptible quota, but
        # cannot be combined with the explicit preemptible flag.
        if preemptible:
            raise AirflowException(
                "preemptible=True is incompatible with FLEX_START provisioning."
            )
        if not max_run_duration_seconds:
            raise AirflowException(
                "max_run_duration_seconds is required for FLEX_START provisioning."
            )
        # Default to the 2h max queue so flex-start actually queues; omitting it
        # would silently fall back to GCP's ~90s fail-fast behavior.
        request_valid_for_duration_seconds = request_valid_for_duration_seconds or 7200
        config["scheduling"] = {
            "provisioningModel": "FLEX_START",
            "onHostMaintenance": "TERMINATE",
            "instanceTerminationAction": "DELETE",
            "maxRunDuration": {"seconds": str(int(max_run_duration_seconds))},
        }
        config["reservationAffinity"] = {"consumeReservationType": "NO_RESERVATION"}
        # Hold the request in the DWS queue for up to this long (max 2h) while the
        # VM sits in PENDING waiting for capacity.
        config["params"] = {
            "requestValidForDuration": {
                "seconds": str(int(request_valid_for_duration_seconds))
            }
        }

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
        provisioning_model: str = "STANDARD",
        max_run_duration_seconds: t.Optional[int] = None,
        request_valid_for_duration_seconds: t.Optional[int] = None,
        reservation_name: t.Optional[str] = None,
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
                    "scopes": [
                        "https://www.googleapis.com/auth/cloud-platform",
                        *self.additional_scopes,
                    ],
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
        self._apply_provisioning_config(
            config,
            provisioning_model=provisioning_model,
            preemptible=preemptible,
            max_run_duration_seconds=max_run_duration_seconds,
            request_valid_for_duration_seconds=request_valid_for_duration_seconds,
            reservation_name=reservation_name,
        )

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
        zone = getattr(failing_task, "gce_zone", GCE_ZONE)

        failing_task.log.info(f"Stopping VM {instance_name} due to task failure.")

        with GCEHook(gce_zone=zone) as hook:
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
