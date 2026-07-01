import subprocess
import typing as t
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from common.config import (
    ENV_SHORT_NAME,
    ENVIRONMENT_NAME,
    GCE_BASE_PREFIX,
    GCE_ZONE,
    GCP_PROJECT_ID,
    LOCAL_ENV,
    SSH_USER,
    USE_INTERNAL_IP,
    UV_VERSION,
)
from common.hooks.gce import DeferrableSSHGCEJobManager, GCEHook, SSHGCEJobManager
from common.hooks.image import MACHINE_TYPE
from common.hooks.network import BASE_NETWORK_LIST, GKE_NETWORK_LIST
from common.triggers.gce import (
    DeferrableSSHJobMonitorTrigger,
    GCEInstanceRunningTrigger,
)


def parse_duration_to_seconds(value: t.Union[str, int, None]) -> t.Optional[int]:
    """Parse a duration into seconds.

    Accepts an int (seconds), a plain numeric string ("3600"), or a compound
    duration string like "1d2h3m4s", "12h", "90m". Returns None for empty input.
    """
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    value = str(value).strip()
    if value.isdigit():
        return int(value)
    units = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    total = 0
    num = ""
    matched = False
    for char in value:
        if char.isdigit():
            num += char
        elif char in units and num:
            total += int(num) * units[char]
            num = ""
            matched = True
        else:
            raise ValueError(f"Invalid duration string: {value!r}")
    if num:  # trailing bare number is treated as seconds
        total += int(num)
        matched = True
    if not matched:
        raise ValueError(f"Invalid duration string: {value!r}")
    return total


class StartGCEOperator(BaseOperator):
    template_fields = [
        "instance_name",
        "instance_type",
        "preemptible",
        "disk_size_gb",
        "labels",
        "use_gke_network",
        "gce_zone",
        "gpu_type",
        "gpu_count",
        "additional_scopes",
        "provisioning_model",
        "max_run_duration",
        "request_valid_for_duration",
    ]

    def __init__(
        self,
        instance_name: str,
        instance_type: str = "n1-standard-1",
        preemptible: bool = False,
        disk_size_gb: str = "100",
        labels={},
        use_gke_network: bool = False,
        gce_zone: str = "europe-west1-b",
        gpu_type: t.Optional[str] = None,
        gpu_count: int = 0,
        additional_scopes: t.List[str] = None,
        provisioning_model: str = "STANDARD",
        max_run_duration: t.Union[str, int, None] = None,
        request_valid_for_duration: t.Union[str, int, None] = None,
        deferrable: bool = False,
        poll_interval: int = 60,
        *args,
        **kwargs,
    ):
        super(StartGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.instance_type = instance_type
        self.preemptible = preemptible
        self.gpu_type = gpu_type
        self.gpu_count = gpu_count
        self.disk_size_gb = disk_size_gb
        self.labels = labels
        self.use_gke_network = use_gke_network
        self.gce_zone = gce_zone
        self.additional_scopes = additional_scopes or []
        self.provisioning_model = provisioning_model
        self.max_run_duration = max_run_duration
        self.request_valid_for_duration = request_valid_for_duration
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def execute(self, context) -> None:
        self.gpu_count = int(self.gpu_count)
        image_type = MACHINE_TYPE["cpu"] if self.gpu_count == 0 else MACHINE_TYPE["gpu"]
        gce_networks = (
            GKE_NETWORK_LIST if self.use_gke_network is True else BASE_NETWORK_LIST
        )
        is_flex_start = (self.provisioning_model or "STANDARD").upper() == "FLEX_START"
        max_run_seconds = parse_duration_to_seconds(self.max_run_duration)
        request_valid_seconds = parse_duration_to_seconds(
            self.request_valid_for_duration
        )
        with GCEHook(
            source_image_type=image_type,
            disk_size_gb=self.disk_size_gb,
            gce_networks=gce_networks,
            gce_zone=self.gce_zone,
            additional_scopes=self.additional_scopes,
        ) as hook:
            # For a deferrable flex-start start we submit the (async, queued) insert
            # and hand off polling to the trigger so the worker slot is freed while
            # the VM is PENDING. Otherwise start_vm blocks until the VM is RUNNING.
            hook.start_vm(
                self.instance_name,
                self.instance_type,
                preemptible=self.preemptible,
                labels=self.labels,
                gpu_type=self.gpu_type,
                gpu_count=self.gpu_count,
                provisioning_model=self.provisioning_model,
                max_run_duration_seconds=max_run_seconds,
                request_valid_for_duration_seconds=request_valid_seconds,
                wait_for_running=not (is_flex_start and self.deferrable),
            )

        if is_flex_start and self.deferrable:
            # Cover the max queue wait plus provisioning/boot margin.
            timeout = (request_valid_seconds or 7200) + 1800
            self.log.info(
                f"Flex-start request for {self.instance_name} submitted; deferring "
                "until the instance reaches RUNNING."
            )
            self.defer(
                trigger=GCEInstanceRunningTrigger(
                    instance_name=self.instance_name,
                    zone=self.gce_zone,
                    poll_interval=self.poll_interval,
                    timeout=timeout,
                ),
                method_name="execute_complete",
            )

    def execute_complete(
        self, context: t.Dict[str, t.Any], event: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        if event is None:
            raise AirflowException("No event received in trigger callback")
        if event.get("status") == "running_ok":
            self.log.info(f"Instance {event.get('instance')} is RUNNING.")
            return
        raise AirflowException(
            event.get("message", "Flex-start instance failed to start")
        )


class CleanGCEOperator(BaseOperator):
    template_fields = [
        "timeout_in_minutes",
    ]

    def __init__(
        self,
        timeout_in_minutes: int,
        job_type: str,
        *args,
        **kwargs,
    ):
        super(CleanGCEOperator, self).__init__(*args, **kwargs)
        self.timeout_in_minutes = timeout_in_minutes
        self.job_type = job_type

    def execute(self, context) -> None:
        with GCEHook() as hook:
            hook.delete_instances(
                job_type=self.job_type, timeout_in_minutes=self.timeout_in_minutes
            )


class DeleteGCEOperator(BaseOperator):
    template_fields = ["instance_name", "gce_zone"]

    def __init__(
        self,
        instance_name: str,
        gce_zone: str = GCE_ZONE,
        *args,
        **kwargs,
    ):
        # Set default priority weight and weight rule for delete operations
        kwargs.setdefault("priority_weight", 1000)
        kwargs.setdefault("weight_rule", "absolute")
        super(DeleteGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.gce_zone = gce_zone

    def execute(self, context):
        # Delete in the zone the VM was created in: deleting in the wrong zone
        # 404s silently and leaves a billable instance running.
        with GCEHook(gce_zone=self.gce_zone) as hook:
            hook.delete_vm(self.instance_name)


class StopGCEOperator(BaseOperator):
    template_fields = ["instance_name", "gce_zone"]

    def __init__(
        self,
        instance_name: str,
        gce_zone: str = GCE_ZONE,
        *args,
        **kwargs,
    ):
        super(StopGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.gce_zone = gce_zone

    def execute(self, context):
        with GCEHook(gce_zone=self.gce_zone) as hook:
            hook.stop_vm(self.instance_name)


class BaseSSHGCEOperator(BaseOperator):
    template_fields = [
        "instance_name",
        "command",
        "environment",
        "gce_zone",
        "deferrable",
        "poll_interval",
    ]

    def __init__(
        self,
        instance_name: str,
        command: str,
        environment: t.Dict[str, str] = {},
        gce_zone=GCE_ZONE,
        deferrable: bool = False,
        poll_interval: int = 300,
        *args,
        **kwargs,
    ):
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.command = command
        self.environment = environment
        self.gce_zone = gce_zone
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        super(BaseSSHGCEOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        ssh_hook = ComputeEngineSSHHook(
            instance_name=self.instance_name,
            zone=self.gce_zone,
            project_id=GCP_PROJECT_ID,
            use_internal_ip=USE_INTERNAL_IP,
            use_iap_tunnel=True,
            use_oslogin=False,
            user=SSH_USER,
            gcp_conn_id="google_cloud_default",
            expire_time=300,
        )
        self.log.info(
            f"Connecting to instance {self.instance_name} in zone {self.gce_zone} with project {GCP_PROJECT_ID}"
        )
        if self.deferrable:
            return self.run_deferrable(context, ssh_hook)
        else:
            return self.run_sync(context, ssh_hook)

    def _run_gcloud_ssh(self, context) -> str:
        """Execute SSH command on GCE instance via gcloud CLI.

        Used for local development where ComputeEngineSSHHook may fail due to
        SSH key propagation and IAP tunnel auth issues. gcloud compute ssh
        handles key management, IAP tunneling, and authentication natively
        using the developer's own gcloud credentials.
        """
        self.log.info(f"Running command:\n{self.command}")

        gcloud_cmd = [
            "gcloud",
            "compute",
            "ssh",
            f"{SSH_USER}@{self.instance_name}",
            f"--project={GCP_PROJECT_ID}",
            f"--zone={self.gce_zone}",
            "--tunnel-through-iap",
            "--quiet",
            "--",
            "bash",
            "-s",
        ]

        process = subprocess.Popen(
            gcloud_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Send the full command via stdin
        process.stdin.write(self.command)
        process.stdin.close()

        # Stream output line by line to the task log
        output_lines = []
        for line in process.stdout:
            line = line.rstrip("\n")
            self.log.info(line)
            output_lines.append(line)

        return_code = process.wait()
        output = "\n".join(output_lines)

        # Push results to XCom for compatibility with SSHGCEJobManager
        task_instance = context.get("task_instance")
        if task_instance:
            task_instance.xcom_push(key="ssh_exit", value=return_code)
            lines = output.split("\n")
            if len(lines) == 0:
                result = ""
            elif len(lines) == 1:
                result = lines[0]
            elif len(lines[-1]) > 0:
                result = lines[-1]
            else:
                result = lines[-2]
            task_instance.xcom_push(key="result", value=result)

        if return_code != 0:
            raise AirflowException(f"SSH command failed with exit code {return_code}")

        return output

    def run_sync(self, context, ssh_hook: ComputeEngineSSHHook):
        job_manager = SSHGCEJobManager(
            task_id=self.task_id,
            task_instance=context["task_instance"],
            hook=ssh_hook,
            environment=self.environment,
        )
        self.log.info(f"Running command: {self.command}")
        return job_manager.run_ssh_client_command(self.command)

    def run_deferrable(self, context, ssh_hook: ComputeEngineSSHHook):
        run_id = str(context.get("run_id", datetime.now().strftime("%Y%m%d%H%M%S")))
        job_manager = DeferrableSSHGCEJobManager(
            task_id=self.task_id,
            run_id=run_id,
            task_instance=context["task_instance"],
            hook=ssh_hook,
            environment=self.environment,
        )
        self.log.info(f"Running command: {self.command}")
        result = job_manager.run_ssh_client_command(self.command)
        job_id = result.strip() if result else None

        if not job_id:
            raise AirflowException("Failed to get job ID after submission")

        # Defer to trigger for monitoring
        self.log.info(f"Job {job_id} submitted, deferring to trigger for monitoring")
        self.defer(
            trigger=DeferrableSSHJobMonitorTrigger(
                task_id=self.task_id,
                instance_name=self.instance_name,
                zone=self.gce_zone,
                run_id=run_id,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: t.Dict[str, t.Any], event: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info(f"Executing complete for job {event}")
        if event is None:
            raise AirflowException("No event received in trigger callback")

        status = event.get("status", "error")
        logs = event.get("logs", "")
        message = event.get("message", "")

        if status == "completed":
            self.log.info("Job completed successfully")
            if logs:
                self.log.info(f"Job logs: {logs}")
            if message:
                self.log.info(f"Job message: {message}")
            return
        elif status == "running":
            # This shouldn't happen as we only get here after the trigger is done
            raise AirflowException("Got running status in trigger callback")
        else:
            self.log.error(
                f"Job {context['task_instance'].run_id} failed with status {status}"
            )
            error_msg = message if message else "Job failed"
            if logs:
                error_msg += f"\nLogs: {logs}"
            raise AirflowException(error_msg)


class SSHGCEOperator(BaseSSHGCEOperator):
    template_fields = ["base_dir"] + BaseSSHGCEOperator.template_fields

    DEFAULT_EXPORT = {
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "ENVIRONMENT_NAME": ENVIRONMENT_NAME,
    }

    UV_EXPORT = {**DEFAULT_EXPORT, "PATH": "$HOME/.local/bin:$PATH"}

    def __init__(
        self,
        instance_name: str,
        command: str,
        base_dir: str = None,
        environment: t.Dict[str, str] = {},
        deferrable: bool = False,
        poll_interval: int = 300,
        *args,
        **kwargs,
    ):
        self.base_dir = base_dir
        self.environment = environment
        self.command = command
        self.instance_name = instance_name
        self.deferrable = deferrable
        self.poll_interval = poll_interval

        super(SSHGCEOperator, self).__init__(
            instance_name=self.instance_name,
            command=self.command,
            environment=self.environment,
            deferrable=self.deferrable,
            poll_interval=self.poll_interval,
            *args,
            **kwargs,
        )

    def prepare_command(self):
        environment = dict(self.UV_EXPORT, **self.environment)
        commands_list = []
        commands_list.append(
            "\n".join([f"export {key}={value}" for key, value in environment.items()])
        )
        if self.base_dir is not None:
            commands_list.append(f"cd ~/{self.base_dir}")

        commands_list.append("source .venv/bin/activate")
        return commands_list

    def execute(self, context):
        commands_list = self.prepare_command()
        commands_list.append(self.command)
        self.command = "\n".join(commands_list)
        return super().execute(context)


class UvxGCEOperator(BaseSSHGCEOperator):
    """Run a CLI tool on a GCE instance via uvx (no repo clone or venv needed)."""

    template_fields = set(
        ["package", "python_version"] + list(BaseSSHGCEOperator.template_fields)
    )

    def __init__(
        self,
        instance_name: str,
        package: str,
        command: str,
        python_version: str = "3.13",
        environment: t.Dict[str, str] = {},
        *args,
        **kwargs,
    ):
        self.package = package
        self.python_version = python_version
        super().__init__(
            instance_name=instance_name,
            command=command,
            environment=environment,
            *args,
            **kwargs,
        )

    def execute(self, context):
        environment = dict(SSHGCEOperator.UV_EXPORT, **self.environment)
        commands_list = [
            "\n".join([f"export {key}={value}" for key, value in environment.items()]),
            f"command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/{UV_VERSION}/install.sh | sh",
            f'uvx --python {self.python_version} --from "{self.package}" {self.command}',
        ]
        self.command = "\n".join(commands_list)
        return super().execute(context)


class InstallDependenciesOperator(SSHGCEOperator):
    REPO = "https://github.com/pass-culture/data-gcp.git"
    template_fields = set(
        [
            "requirement_file",
            "branch",
            "instance_name",
            "base_dir",
            "python_version",
        ]
        + SSHGCEOperator.template_fields
    )

    def __init__(
        self,
        instance_name: str,
        requirement_file: str = "requirements.txt",
        branch: str = "master",  # Branch for repo
        environment: t.Dict[str, str] = {},
        python_version: str = "3.10",
        base_dir: str = "data-gcp",
        *args,
        **kwargs,
    ):
        self.instance_name = instance_name
        self.requirement_file = requirement_file
        self.environment = environment
        self.python_version = python_version
        self.branch = branch
        self.base_dir = base_dir
        # Call the parent class constructor but do not pass the command yet
        super(InstallDependenciesOperator, self).__init__(
            instance_name=self.instance_name,
            command="",  # Placeholder command
            environment=self.environment,
            base_dir=self.base_dir,  # Pass base_dir to parent class
            *args,
            **kwargs,
        )

    def execute(self, context):
        command = self.make_install_command(
            self.requirement_file, self.branch, self.base_dir
        )
        self.command = command

        if LOCAL_ENV:
            # Locally, use gcloud compute ssh which natively handles SSH key
            # propagation and IAP tunnel auth via the developer's own gcloud
            # credentials. This first connection pushes SSH keys to the VM
            # metadata, allowing subsequent ComputeEngineSSHHook calls to work.
            commands_list = self.prepare_command()
            commands_list.append(self.command)
            self.command = "\n".join(commands_list)
            self.log.info(
                f"Local environment detected — using gcloud compute ssh "
                f"for instance {self.instance_name} in zone {self.gce_zone}"
            )
            return self._run_gcloud_ssh(context)

        return super(InstallDependenciesOperator, self).execute(context)

    def make_install_command(
        self,
        requirement_file: str,
        branch: str,
        base_dir: str = "data-gcp",
    ) -> str:
        """
        Construct the command to clone the repo and install dependencies.
        """
        # Define the directory where the repo will be cloned
        REPO_DIR = "data-gcp"

        # Git clone command
        clone_command = f"""
            cd ~/ &&
            DIR={REPO_DIR} &&
            if [ -d "$DIR" ]; then
                echo "Directory exists. Fetching updates..." &&
                cd $DIR &&
                git fetch --all &&
                git reset --hard origin/{branch};
            else
                echo "Cloning repository..." &&
                git clone {self.REPO} $DIR &&
                cd $DIR &&
                git checkout {branch};
            fi &&
            cd ~/
        """

        install_command = f"""
            curl -LsSf https://astral.sh/uv/{UV_VERSION}/install.sh | sh &&
            cd {base_dir} &&
            uv venv --python {self.python_version} &&
            source .venv/bin/activate &&
            if [ -f "{requirement_file}" ]; then
                uv pip sync {requirement_file}
            else
                uv sync
            fi
        """

        deactivate_conda = (
            "echo 'conda config --set auto_activate_base false' >> ~/.bashrc"
        )

        # Combine the git clone and installation commands
        return f"""
            set -e
            {clone_command}
            {install_command}
            {deactivate_conda}
        """
