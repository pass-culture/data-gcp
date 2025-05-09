import asyncio
import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Type

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
        job_id: str,
        poll_interval: int = 60,
        timeout: Optional[int] = None,
        ssh_hook_cls: Type[ComputeEngineSSHHook] = ComputeEngineSSHHook,
    ):
        super().__init__()
        self.task_id = task_id
        self.instance_name = instance_name
        self.zone = zone
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.timeout = timeout
        self._start_time = datetime.utcnow()
        self._job_manager = DeferrableSSHGCEJobManager(task_id, job_id)
        self._ssh_hook_cls = ssh_hook_cls

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "common.triggers.gce.DeferrableSSHJobMonitorTrigger",
            {
                "task_id": self.task_id,
                "instance_name": self.instance_name,
                "zone": self.zone,
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    @classmethod
    def deserialize(
        cls, trigger_data: Dict[str, Any]
    ) -> "DeferrableSSHJobMonitorTrigger":
        return cls(
            task_id=trigger_data["task_id"],
            instance_name=trigger_data["instance_name"],
            zone=trigger_data["zone"],
            job_id=trigger_data["job_id"],
            poll_interval=trigger_data["poll_interval"],
            timeout=trigger_data.get("timeout"),
        )

    async def run(self):
        """
        Polls the SSH job status until completion, failure, interruption, or timeout,
        yielding a TriggerEvent on each check.
        """
        hook = self._ssh_hook_cls(
            instance_name=self.instance_name,
            zone=self.zone,
            project_id=GCP_PROJECT_ID,
            use_iap_tunnel=True,
            use_oslogin=False,
            user=SSH_USER,
        )

        while True:
            # Check overall timeout
            if self.timeout:
                elapsed = (datetime.utcnow() - self._start_time).total_seconds()
                if elapsed > self.timeout:
                    yield TriggerEvent(
                        {
                            "status": "timeout",
                            "job_id": self.job_id,
                            "instance": self.instance_name,
                            "message": "Exceeded monitoring timeout",
                        }
                    )
                    return

            try:
                with hook.get_conn() as ssh_client:
                    exit_code, stdout, stderr = hook.exec_ssh_client_command(
                        ssh_client,
                        self._job_manager.get_status_check_command(),
                        timeout=30,
                    )

                raw = stdout.decode("utf-8", errors="replace")
                status, output = self._parse(raw)

                if status is None:
                    self.log.warning(f"Malformed STATUS in job output: {raw}")
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "job_id": self.job_id,
                            "message": "Could not parse STATUS",
                            "logs": output,
                        }
                    )
                    return

                # Job finished
                if status in {"completed", "failed", "interrupted"}:
                    # attempt cleanup
                    try:
                        with hook.get_conn() as ssh_client:
                            hook.exec_ssh_client_command(
                                ssh_client,
                                self._job_manager.get_cleanup_command(),
                                timeout=30,
                            )
                    except Exception as cleanup_err:
                        self.log.warning(f"Cleanup failed: {cleanup_err}")

                    yield TriggerEvent(
                        {
                            "status": "completed" if status == "completed" else "error",
                            "job_id": self.job_id,
                            "instance": self.instance_name,
                            "logs": output,
                        }
                    )
                    return

                # Still running
                yield TriggerEvent(
                    {
                        "status": "running",
                        "job_id": self.job_id,
                        "instance": self.instance_name,
                        "logs": self._truncate(output, 500),
                    }
                )

            except Exception as exc:
                self.log.error(f"Error monitoring job {self.job_id}: {exc}")
                yield TriggerEvent(
                    {
                        "status": "error",
                        "job_id": self.job_id,
                        "message": str(exc),
                    }
                )
                return

            await asyncio.sleep(self.poll_interval)

    @staticmethod
    def _parse(raw: str) -> Tuple[Optional[str], str]:
        """
        Extracts STATUS and OUTPUT from raw SSH output.
        Returns (status_str or None, output_text).
        """
        m_status = re.search(r"STATUS:(\w+)", raw)
        m_output = re.search(r"OUTPUT:(.*)", raw, re.DOTALL)
        status = m_status.group(1).lower() if m_status else None
        output = m_output.group(1).strip() if m_output else "No output available"
        return status, output

    @staticmethod
    def _truncate(text: str, length: int) -> str:
        """Return only the last `length` characters of text."""
        return text[-length:] if len(text) > length else text
