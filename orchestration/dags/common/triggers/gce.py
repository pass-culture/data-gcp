import asyncio
import re
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from common.config import GCP_PROJECT_ID, SSH_USER
from common.hooks.gce import DeferrableSSHGCEJobManager
from paramiko.ssh_exception import SSHException

from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class JobState(str, Enum):
    """Possible states of a GCE job."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"

    def is_terminal(self) -> bool:
        """Check if this is a terminal state."""
        return self in {self.COMPLETED, self.FAILED, self.INTERRUPTED}


class SSHCommandError(Exception):
    """Raised when an SSH command fails."""

    pass


@dataclass
class JobStatus:
    """Represents the status of a job running on GCE."""

    status: JobState
    logs: str
    pid: str
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the status to a dictionary, excluding None values."""
        data = asdict(self)
        data["status"] = self.status.value  # Convert enum to string
        return {k: v for k, v in data.items() if v is not None}


class DeferrableSSHJobMonitorTrigger(BaseTrigger):
    """
    Trigger for monitoring remote SSH jobs on GCE instances.

    This trigger periodically checks the status of a long-running SSH job
    and triggers completion when the job finishes.

    :param task_id: The task ID of the deferrable task
    :param instance_name: Name of the GCE instance
    :param zone: GCE zone where the instance is located
    :param job_id: Unique identifier for the job being monitored
    :param poll_interval: How often to check job status in seconds
    """

    MAX_RETRY = 3
    SSH_TIMEOUT = 10

    def __init__(
        self,
        task_id: str,
        instance_name: str,
        zone: str,
        job_id: str,
        poll_interval: int = 60,
    ):
        super().__init__()
        self.task_id = task_id
        self.instance_name = instance_name
        self.zone = zone
        self.job_id = job_id
        self.poll_interval = poll_interval
        self._hook = None
        self._job_manager = DeferrableSSHGCEJobManager(task_id, job_id)

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes the trigger for storage in the database."""
        return (
            "common.triggers.gce.DeferrableSSHJobMonitorTrigger",
            {
                "task_id": self.task_id,
                "instance_name": self.instance_name,
                "zone": self.zone,
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
            },
        )

    @classmethod
    def deserialize(
        cls, trigger_data: Dict[str, Any]
    ) -> "DeferrableSSHJobMonitorTrigger":
        """Deserializes a trigger from the database."""
        return cls(
            task_id=trigger_data["task_id"],
            instance_name=trigger_data["instance_name"],
            zone=trigger_data["zone"],
            job_id=trigger_data["job_id"],
            poll_interval=trigger_data["poll_interval"],
        )

    @property
    def hook(self) -> ComputeEngineSSHHook:
        """Lazy initialization of SSH hook."""
        if self._hook is None:
            self._hook = ComputeEngineSSHHook(
                instance_name=self.instance_name,
                zone=self.zone,
                project_id=GCP_PROJECT_ID,
                use_iap_tunnel=True,
                use_oslogin=False,
                user=SSH_USER,
            )
        return self._hook

    async def run_ssh_command(
        self, command: str, timeout: int = 30, retry: int = 1
    ) -> tuple[int, bytes, bytes]:
        """
        Execute an SSH command with retry logic.

        Args:
            command: The command to execute
            timeout: Command timeout in seconds
            retry: Current retry attempt number

        Returns:
            Tuple of (exit_status, stdout, stderr)

        Raises:
            SSHCommandError: If command fails after all retries
        """
        try:
            with self.hook.get_conn() as ssh_client:
                exit_status, stdout, stderr = self.hook.exec_ssh_client_command(
                    ssh_client,
                    command,
                    timeout=timeout,
                    get_pty=False,
                )

                if exit_status != 0:
                    raise SSHCommandError(
                        f"SSH command failed with exit status {exit_status}. "
                        f"stderr: {stderr.decode('utf-8')}"
                    )

                return exit_status, stdout, stderr

        except (SSHException, SSHCommandError) as e:
            self.log.info(
                f"Cannot connect to instance {self.instance_name}. "
                f"Retry {retry}/{self.MAX_RETRY}"
            )
            if retry >= self.MAX_RETRY:
                self.log.error(
                    f"Failed to execute SSH command after {retry} retries. "
                    f"Last error: {str(e)}"
                )
                raise SSHCommandError(
                    f"SSH command failed after {retry} retries: {str(e)}"
                )

            # Exponential backoff
            sleep_time = retry * self.SSH_TIMEOUT
            self.log.info(f"Waiting {sleep_time} seconds before retry...")
            await asyncio.sleep(sleep_time)

            return await self.run_ssh_command(
                command=command, timeout=timeout, retry=retry + 1
            )

    async def check_job_status(self) -> JobStatus:
        """
        Check the status of the remote job.

        Returns:
            JobStatus containing status, logs, and process ID of the job
        """
        try:
            _, stdout, _ = await self.run_ssh_command(
                command=self._job_manager.get_status_check_command(), timeout=30
            )

            output = stdout.decode("utf-8")
            status_str = re.search(r"STATUS:(\w+)", output).group(1)
            job_output = re.search(r"OUTPUT:(.*)", output, re.DOTALL).group(1)
            pid = re.search(r"PID:(\d+)", output).group(1)

            try:
                status = JobState(status_str)
            except ValueError:
                self.log.error(f"Invalid job status received: {status_str}")
                status = JobState.FAILED

            self.log.info(f"Job {self.job_id} status: {status.value}")
            self.log.debug(f"Job {self.job_id} output: {job_output}")

            return JobStatus(status=status, logs=job_output, pid=pid)

        except Exception as e:
            self.log.error(f"Error checking status for job {self.job_id}: {str(e)}")
            return JobStatus(
                status=JobState.FAILED,
                logs="",
                pid="0",
                message=f"Status check failed: {str(e)}",
            )

    async def cleanup_job(self) -> None:
        """Clean up job directory after completion or failure."""
        try:
            await self.run_ssh_command(
                command=self._job_manager.get_cleanup_command(), timeout=30
            )
            self.log.info(f"Successfully cleaned up job {self.job_id}")
        except Exception as e:
            self.log.warning(f"Failed to cleanup job {self.job_id}: {str(e)}")

    async def run(self) -> TriggerEvent:
        """
        Main trigger loop that monitors the job status.

        Returns:
            TriggerEvent containing the final job status and output
        """
        while True:
            try:
                result = await self.check_job_status()

                if result.status.is_terminal():
                    await self.cleanup_job()
                    return TriggerEvent(result.to_dict())

                self.log.info(
                    f"Job {self.job_id} still running, "
                    f"checking again in {self.poll_interval} seconds"
                )
                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                self.log.error(f"Error in trigger run loop: {str(e)}")
                error_status = JobStatus(
                    status=JobState.FAILED,
                    logs="",
                    pid="0",
                    message=f"Trigger error: {str(e)}",
                )
                return TriggerEvent(error_status.to_dict())
