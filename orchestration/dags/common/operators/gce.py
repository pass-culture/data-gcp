import typing as t
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.utils.decorators import apply_defaults
from common.config import (
    ENV_SHORT_NAME,
    ENVIRONMENT_NAME,
    GCE_BASE_PREFIX,
    GCE_ZONE,
    GCP_PROJECT_ID,
    SSH_USER,
    USE_INTERNAL_IP,
    UV_VERSION,
)
from common.hooks.gce import DeferrableSSHGCEJobManager, GCEHook, SSHGCEJobManager
from common.hooks.image import MACHINE_TYPE
from common.hooks.network import BASE_NETWORK_LIST, GKE_NETWORK_LIST
from common.triggers.gce import DeferrableSSHJobMonitorTrigger


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
    ]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        instance_type: str = "n1-standard-1",
        preemptible: bool = True,
        disk_size_gb: str = "100",
        labels={},
        use_gke_network: bool = False,
        gce_zone: str = "europe-west1-b",
        gpu_type: t.Optional[str] = None,
        gpu_count: int = 0,
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

    def execute(self, context) -> None:
        image_type = MACHINE_TYPE["cpu"] if self.gpu_count == 0 else MACHINE_TYPE["gpu"]
        gce_networks = (
            GKE_NETWORK_LIST if self.use_gke_network is True else BASE_NETWORK_LIST
        )
        hook = GCEHook(
            source_image_type=image_type,
            disk_size_gb=self.disk_size_gb,
            gce_networks=gce_networks,
            gce_zone=self.gce_zone,
        )
        hook.start_vm(
            self.instance_name,
            self.instance_type,
            preemptible=self.preemptible,
            labels=self.labels,
            gpu_type=self.gpu_type,
            gpu_count=self.gpu_count,
        )


class CleanGCEOperator(BaseOperator):
    template_fields = ["timeout_in_minutes"]

    @apply_defaults
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
        hook = GCEHook()
        hook.delete_instances(
            job_type=self.job_type, timeout_in_minutes=self.timeout_in_minutes
        )


class DeleteGCEOperator(BaseOperator):
    template_fields = ["instance_name"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        *args,
        **kwargs,
    ):
        super(DeleteGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"

    def execute(self, context):
        hook = GCEHook()
        hook.delete_vm(self.instance_name)


class StopGCEOperator(BaseOperator):
    template_fields = ["instance_name"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        *args,
        **kwargs,
    ):
        super(StopGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"

    def execute(self, context):
        hook = GCEHook()
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

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        environment: t.Dict[str, str] = {},
        gce_zone=GCE_ZONE,
        deferrable: bool = False,
        do_xcom_push: bool = False,
        poll_interval: int = 300,
        *args,
        **kwargs,
    ):
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.command = command
        self.environment = environment
        self.gce_zone = gce_zone
        self.deferrable = deferrable
        self.do_xcom_push = do_xcom_push
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
        stdout = event.get("stdout", "")
        stderr = event.get("stderr", "")
        if status == "completed":
            self.log.info("Job completed successfully")
            if logs:
                self.log.info(f"Job logs: {logs}")
            if message:
                self.log.info(f"Job message: {message}")

            self.log.info(f"Job stderr: {stderr}")
            self.log.info(f"Job stdout: {stdout}")
            if self.do_xcom_push and stdout:
                context["ti"].xcom_push(key="stdout", value=stdout)
                context["ti"].xcom_push(key="stderr", value=stderr)
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
            if stderr:
                error_msg += f"\nStderr: {stderr}"
            raise AirflowException(error_msg)


class SSHGCEOperator(BaseSSHGCEOperator):
    template_fields = ["base_dir"] + BaseSSHGCEOperator.template_fields

    DEFAULT_EXPORT = {
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "ENVIRONMENT_NAME": ENVIRONMENT_NAME,
    }

    UV_EXPORT = {**DEFAULT_EXPORT, "PATH": "$HOME/.local/bin:$PATH"}

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        base_dir: str = None,
        environment: t.Dict[str, str] = {},
        deferrable: bool = False,
        do_xcom_push: bool = False,
        poll_interval: int = 300,
        *args,
        **kwargs,
    ):
        self.base_dir = base_dir
        self.environment = environment
        self.command = command
        self.instance_name = instance_name
        self.deferrable = deferrable
        self.do_xcom_push = do_xcom_push
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

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        requirement_file: str = "requirements.txt",
        branch: str = "main",  # Branch for repo
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
        # Combine the git clone and installation commands
        return f"""
            {clone_command}
            {install_command}
        """
