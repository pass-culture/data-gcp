import asyncio
from typing import Any, Dict, Tuple

from common.config import GCP_PROJECT_ID, SSH_USER
from common.hooks.gce import DeferrableSSHGCEJobManager

from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


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
    ):
        self.task_id = task_id
        self.instance_name = instance_name
        self.zone = zone
        self.run_id = run_id
        self.poll_interval = poll_interval

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

        while True:
            try:
                self.log.info(f"Checking job {self.run_id} status")
                deferrable_status = await job_manager.run_ssh_status_check_command()
                self.log.info(
                    f"\t\t Deferrable job {self.run_id} STATUS: {deferrable_status.status}"
                )

                # Job finished
                if deferrable_status.status in {"completed", "failed", "interrupted"}:
                    # attempt cleanup
                    try:
                        await job_manager.run_ssh_cleanup_command()
                    except Exception as cleanup_err:
                        self.log.warning(f"Cleanup failed: {cleanup_err}")

                    yield TriggerEvent(
                        {
                            "status": "completed"
                            if deferrable_status.status == "completed"
                            else "error",
                            "run_id": self.run_id,
                            "instance": self.instance_name,
                            "logs": deferrable_status.logs,
                        }
                    )

            except Exception as exc:
                self.log.error(f"Error monitoring job {self.run_id}: {exc}")
                yield TriggerEvent(
                    {
                        "status": "error",
                        "run_id": self.run_id,
                        "message": str(exc),
                    }
                )

            await asyncio.sleep(self.poll_interval)

    @staticmethod
    def _truncate(text: str, length: int) -> str:
        """Return only the last `length` characters of text."""
        return text[-length:] if len(text) > length else text
