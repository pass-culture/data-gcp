import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Tuple

from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from common.config import GCP_PROJECT_ID, SSH_USER, USE_INTERNAL_IP
from common.hooks.gce import DeferrableSSHGCEJobManager, GCEHook
from googleapiclient.errors import HttpError


class GCEInstanceRunningTrigger(BaseTrigger):
    """Polls a (flex-start) GCE instance until it reaches RUNNING.

    Used by the deferrable ``StartGCEOperator`` so the Airflow worker slot is
    freed while a DWS flex-start request sits queued in PENDING waiting for GPU
    capacity (which can take up to the configured ``request_valid_for_duration``,
    max 2h).
    """

    TERMINAL_STATES = GCEHook.TERMINAL_STATES
    FATAL_HTTP_STATUSES = {401, 403}

    def __init__(
        self,
        instance_name: str,
        zone: str,
        poll_interval: int = 120,
        deadline: float = 0.0,
    ):
        super().__init__()
        self.instance_name = instance_name
        self.zone = zone
        self.poll_interval = poll_interval
        # Absolute epoch deadline (not a relative timeout): this survives a
        # triggerer restart, which re-instantiates the trigger from `serialize()`
        # and would otherwise silently reset a relative-duration countdown.
        self.deadline = deadline

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__name__,
            {
                "instance_name": self.instance_name,
                "zone": self.zone,
                "poll_interval": self.poll_interval,
                "deadline": self.deadline,
            },
        )

    async def run(self):
        loop = asyncio.get_event_loop()
        # Build the hook once, outside the poll loop: constructing GCEHook
        # resolves credentials/discovery docs, which is wasted work (and a
        # possible source of transient errors) if repeated every poll tick.
        # The underlying httplib2/googleapiclient transport is NOT thread-safe,
        # and the triggerer's default executor is a shared, multi-threaded pool
        # used by every concurrently-running trigger in the process — so a
        # single reused hook must be pinned to one dedicated worker thread,
        # otherwise different poll ticks can land on different threads and
        # crash the whole triggerer process (observed as a native.
        hook = GCEHook(gce_zone=self.zone)
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            while True:
                try:
                    instance = await loop.run_in_executor(
                        executor, hook.get_instance, self.instance_name
                    )
                except HttpError as exc:
                    if (
                        exc.resp is not None
                        and exc.resp.status in self.FATAL_HTTP_STATUSES
                    ):
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "instance": self.instance_name,
                                "message": (
                                    f"Failed to check status of {self.instance_name}: "
                                    f"{exc}"
                                ),
                            }
                        )
                        return
                    self.log.warning(
                        f"Transient error checking {self.instance_name} status: "
                        f"{exc}. Retrying."
                    )
                    if time.time() > self.deadline:
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "instance": self.instance_name,
                                "message": (
                                    f"Timed out waiting for {self.instance_name} to "
                                    "reach RUNNING (last check failed with a transient "
                                    "error)."
                                ),
                            }
                        )
                        return
                    await asyncio.sleep(self.poll_interval)
                    continue

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
                if time.time() > self.deadline:
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
        finally:
            hook.close()
            executor.shutdown(wait=False)


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
