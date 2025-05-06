import asyncio

from common.config import GCP_PROJECT_ID, SSH_USER

from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class SSHJobMonitorTrigger(BaseTrigger):
    def __init__(self, task_id, instance_name, zone, command_dir, poll_interval=60):
        self.task_id = task_id
        self.instance_name = instance_name
        self.zone = zone
        self.command_dir = command_dir
        self.poll_interval = poll_interval

    def serialize(self):
        return (
            "common.trigger.gce.SSHJobMonitorTrigger",
            {
                "task_id": self.task_id,
                "instance_name": self.instance_name,
                "zone": self.zone,
                "command_dir": self.command_dir,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        hook = ComputeEngineSSHHook(
            instance_name=self.instance_name,
            zone=self.zone,
            project_id=GCP_PROJECT_ID,
            use_iap_tunnel=True,
            use_oslogin=False,
            user=SSH_USER,
            gcp_conn_id="google_cloud_default",
            expire_time=300,
        )

        while True:
            try:
                with hook.get_conn() as ssh_client:
                    self.log.info(f"Checking for done flag on {self.instance_name}")
                    # Check for done flag
                    stdin, stdout, _ = ssh_client.exec_command(
                        f"cd ~/{self.command_dir} && test -f job_done.flag && echo DONE"
                    )
                    status = stdout.read().decode().strip()

                    if status == "DONE":
                        stdin, stdout, _ = ssh_client.exec_command(
                            f"cd ~/{self.command_dir} && cat job.log"
                        )
                        full_logs = stdout.read().decode()
                        yield TriggerEvent({"status": "completed", "logs": full_logs})
                        return

                    # If still running, tail logs
                    stdin, stdout, _ = ssh_client.exec_command(
                        f"cd ~/{self.command_dir} && tail -n 10 job.log"
                    )
                    partial_logs = stdout.read().decode()
                    self.log.info(f"Partial logs: {partial_logs}")
                    yield TriggerEvent({"status": "running", "logs": partial_logs})

            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})
                return

            await asyncio.sleep(self.poll_interval)
