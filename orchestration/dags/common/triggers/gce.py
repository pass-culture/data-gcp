import asyncio
import time
from typing import Any, Dict, Tuple

from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from common.config import GCP_PROJECT_ID, SSH_USER, USE_INTERNAL_IP
from common.hooks.gce import DeferrableSSHGCEJobManager, GCEHook


class GCEInstanceRunningTrigger(BaseTrigger):
    """Polls a (flex-start) GCE instance until it reaches RUNNING.

    Used by the deferrable ``StartGCEOperator`` so the Airflow worker slot is
    freed while a DWS flex-start request sits queued in PENDING waiting for GPU
    capacity (which can take up to the configured ``request_valid_for_duration``,
    max 2h).
    """

    TERMINAL_STATES = {"TERMINATED", "SUSPENDED", "SUSPENDING", "STOPPING"}

    def __init__(
        self,
        instance_name: str,
        zone: str,
        poll_interval: int = 60,
        timeout: int = 9000,
    ):
        super().__init__()
        self.instance_name = instance_name
        self.zone = zone
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__name__,
            {
                "instance_name": self.instance_name,
                "zone": self.zone,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    def _get_instance(self):
        with GCEHook(gce_zone=self.zone) as hook:
            return hook.get_instance(self.instance_name)

    async def run(self):
        loop = asyncio.get_event_loop()
        deadline = time.time() + self.timeout
        while True:
            try:
                instance = await loop.run_in_executor(None, self._get_instance)
            except Exception as exc:  # transient API error: log and retry
                self.log.warning(
                    f"Error checking {self.instance_name} status: {exc}. Retrying."
                )
                instance = {"status": "UNKNOWN"}

            if instance is None:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "instance": self.instance_name,
                        "message": (
                            f"Instance {self.instance_name} no longer exists: the "
                            "flex-start request likely expired without securing "
                            "capacity."
                        ),
                    }
                )
                return

            status = instance.get("status")
            self.log.info(f"Instance {self.instance_name} status: {status}")

            if status == "RUNNING":
                yield TriggerEvent(
                    {"status": "running_ok", "instance": self.instance_name}
                )
                return
            if status in self.TERMINAL_STATES:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "instance": self.instance_name,
                        "message": (
                            f"Instance {self.instance_name} reached terminal state "
                            f"{status} before RUNNING; flex-start failed to secure "
                            "capacity."
                        ),
                    }
                )
                return
            if time.time() > deadline:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "instance": self.instance_name,
                        "message": (
                            f"Timed out waiting for {self.instance_name} to reach "
                            f"RUNNING (last status: {status})."
                        ),
                    }
                )
                return

            await asyncio.sleep(self.poll_interval)


class DeferrableSSHJobMonitorTrigger(BaseTrigger):
    """
    Trigger that monitors a remote SSH job on a GCE instance and emits TriggerEvent.
    """

    def __init__(
        self,
        task_id: str,
        instance_name: str,
        zone: str,
        run_id: str,
        poll_interval: int = 300,
        max_log_length: int = 5000,
    ):
        self.task_id = task_id
        self.instance_name = instance_name
        self.zone = zone
        self.run_id = run_id
        self.poll_interval = poll_interval
        self.max_log_length = max_log_length

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__name__,
            {
                "task_id": self.task_id,
                "instance_name": self.instance_name,
                "zone": self.zone,
                "run_id": self.run_id,
                "poll_interval": self.poll_interval,
            },
        )

    @staticmethod
    def get_hook(instance_name, zone):
        return ComputeEngineSSHHook(
            instance_name=instance_name,
            zone=zone,
            project_id=GCP_PROJECT_ID,
            use_internal_ip=USE_INTERNAL_IP,
            use_iap_tunnel=True,
            use_oslogin=False,
            user=SSH_USER,
            gcp_conn_id="google_cloud_default",
            expire_time=300,
        )

    async def run(self):
        """
        Polls the SSH job status until completion, failure, interruption, or timeout,
        yielding a TriggerEvent on each check.
        """
        self.log.info(
            f"Running job {self.run_id} on instance {self.instance_name} in zone {self.zone}"
        )
        hook = self.get_hook(self.instance_name, self.zone)
        job_manager = DeferrableSSHGCEJobManager(
            task_id=self.task_id,
            run_id=self.run_id,
            task_instance=self.task_instance,
            hook=hook,
            environment={},
        )
        try:
            while True:
                try:
                    self.log.info(f"Checking job {self.run_id} status")
                    deferrable_status = await job_manager.run_ssh_status_check_command()
                    self.log.info(
                        f"\t\t Deferrable job {self.run_id} STATUS: {deferrable_status.status}"
                    )

                    # Retry once on "directory not found" to guard against transient
                    # filesystem visibility delays at trigger startup.
                    if (
                        deferrable_status.status == "failed"
                        and "Job directory not found" in deferrable_status.output
                    ):
                        self.log.warning(
                            f"Job directory not found for {self.run_id}, retrying in 30s "
                            f"(transient filesystem visibility or triggerer restart)"
                        )
                        await asyncio.sleep(30)
                        deferrable_status = (
                            await job_manager.run_ssh_status_check_command()
                        )
                        self.log.info(
                            f"\t\t Deferrable job {self.run_id} STATUS after retry: {deferrable_status.status}"
                        )

                    # Job finished
                    if deferrable_status.status in {
                        "completed",
                        "failed",
                        "interrupted",
                    }:
                        # attempt cleanup
                        try:
                            await job_manager.run_ssh_cleanup_command()
                        except Exception as cleanup_err:
                            self.log.warning(f"Cleanup failed: {cleanup_err}")
                        truncated_logs = self._truncate(
                            deferrable_status.logs, self.max_log_length
                        )
                        yield TriggerEvent(
                            {
                                "status": "completed"
                                if deferrable_status.status == "completed"
                                else "error",
                                "run_id": self.run_id,
                                "instance": self.instance_name,
                                "logs": truncated_logs,
                            }
                        )
                        break

                except Exception as exc:
                    self.log.error(f"Error monitoring job {self.run_id}: {exc}")
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "run_id": self.run_id,
                            "message": str(exc),
                        }
                    )
                    break

                await asyncio.sleep(self.poll_interval)

        finally:
            # Cleanup regardless of success or failure
            try:
                await job_manager.run_ssh_cleanup_command()
            except Exception as cleanup_err:
                self.log.warning(f"Cleanup failed in finally: {cleanup_err}")
            job_manager = None
            hook = None

    @staticmethod
    def _truncate(text: str, length: int) -> str:
        """Return only the last `length` characters of text."""
        return text[-length:] if len(text) > length else text
