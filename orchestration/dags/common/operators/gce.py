import typing as t
from base64 import b64encode
from time import sleep

from common.config import (
    ENV_SHORT_NAME,
    ENVIRONMENT_NAME,
    GCE_BASE_PREFIX,
    GCE_ZONE,
    GCP_PROJECT_ID,
    SSH_USER,
    UV_VERSION,
)
from common.hooks.gce import GCEHook
from common.hooks.image import MACHINE_TYPE
from common.hooks.network import BASE_NETWORK_LIST, GKE_NETWORK_LIST
from orchestration.dags.common.trigger.gce import SSHJobMonitorTrigger
from paramiko.ssh_exception import SSHException

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.utils.decorators import apply_defaults


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
    MAX_RETRY = 3
    SSH_TIMEOUT = 10
    template_fields = ["instance_name", "command", "environment", "gce_zone"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        environment: t.Dict[str, str] = {},
        gce_zone=GCE_ZONE,
        *args,
        **kwargs,
    ):
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.command = command
        self.environment = environment
        self.gce_zone = gce_zone
        super(BaseSSHGCEOperator, self).__init__(*args, **kwargs)

    def run_ssh_client_command(self, hook, context, retry=1):
        try:
            with hook.get_conn() as ssh_client:
                exit_status, agg_stdout, agg_stderr = hook.exec_ssh_client_command(
                    ssh_client,
                    self.command,
                    timeout=3600,
                    environment=self.environment,
                    get_pty=False,
                )
                if context and self.do_xcom_push:
                    ti = context.get("task_instance")
                    ti.xcom_push(key="ssh_exit", value=exit_status)

                    # Ensure there are enough lines in the output
                    lines_result = agg_stdout.decode("utf-8").split("\n")
                    self.log.info(f"Lines result: {lines_result}")
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
                    ti.xcom_push(key="result", value=result)

                if exit_status != 0:
                    raise AirflowException(
                        f"SSH operator error: exit status = {exit_status}"
                    )
                return agg_stdout
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
            self.run_ssh_client_command(hook, context, retry=retry + 1)

    def execute(self, context):
        self.log.info(
            f"Connecting to instance {self.instance_name} in zone {self.gce_zone} with project {GCP_PROJECT_ID}"
        )
        hook = ComputeEngineSSHHook(
            instance_name=self.instance_name,
            zone=self.gce_zone,
            project_id=GCP_PROJECT_ID,
            use_iap_tunnel=True,
            use_oslogin=False,
            user=SSH_USER,
            gcp_conn_id="google_cloud_default",
            expire_time=300,
        )
        result = self.run_ssh_client_command(hook, context)
        enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
        if not enable_pickling:
            if result is not None:
                result = b64encode(result).decode("utf-8")
        return result


class CloneRepositoryGCEOperator(BaseSSHGCEOperator):
    REPO = "https://github.com/pass-culture/data-gcp.git"

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        environment: t.Dict[str, str] = {},
        python_version: str = "3.10",
        use_uv: bool = True,
        *args,
        **kwargs,
    ):
        self.use_uv = use_uv
        self.command = self.clone_and_init_with_uv(command, python_version)

        self.instance_name = instance_name
        self.environment = environment
        self.python_version = python_version
        super(CloneRepositoryGCEOperator, self).__init__(
            instance_name=self.instance_name,
            command=self.command,
            environment=self.environment,
            *args,
            **kwargs,
        )

    def clone_and_init_with_uv(self, branch, python_version) -> str:
        return f"""
        curl -LsSf https://astral.sh/uv/{UV_VERSION}/install.sh | sh
        uv venv --python {python_version}
        DIR=data-gcp &&
        if [ -d "$DIR" ]; then
            echo "Update and Checkout repo..." &&
            cd $DIR &&
            git fetch --all &&
            git reset --hard origin/{branch}
        else
            echo "Clone and checkout repo..." &&
            git clone {self.REPO} &&
            cd $DIR &&
            git checkout {branch}
        fi
        """


class SSHGCEOperator(BaseSSHGCEOperator):
    template_fields = [
        "instance_name",
        "command",
        "environment",
        "gce_zone",
        "base_dir",
    ]
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
        *args,
        **kwargs,
    ):
        self.base_dir = base_dir
        self.environment = environment
        self.command = command
        self.instance_name = instance_name

        super(SSHGCEOperator, self).__init__(
            instance_name=self.instance_name,
            command=self.command,
            environment=self.environment,
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

    def nohup_command(self, context):
        commands_list = self.prepare_command()
        commands_list.append(
            f"nohup {self.command} > job.log 2>&1 && touch job_done.flag & echo $! > job.pid"
        )
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
        # The templates have been rendered; we can construct the command
        command = self.make_install_command(
            self.requirement_file, self.branch, self.base_dir
        )
        # Use the command in the parent SSHGCEOperator to execute on the remote instance
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


class DeferrableSSHGCEOperator(SSHGCEOperator):
    def execute(self, context):
        self.nohup_command(context)

        self.defer(
            trigger=SSHJobMonitorTrigger(
                task_id=self.task_id,
                instance_name=self.instance_name,
                zone=self.gce_zone,
                command_dir=self.base_dir,
                poll_interval=60,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        if event is None:
            raise AirflowException("No trigger event received.")

        status = event.get("status")
        if status == "completed":
            self.log.info("Job completed. Full logs:\n" + event["logs"])
            context["ti"].xcom_push(key="full_logs", value=event["logs"])
        elif status == "running":
            self.log.info("Job still running. Partial logs:\n" + event["logs"])
            raise AirflowException(
                "Unexpected status: running. Should not return this."
            )
        else:
            raise AirflowException(f"Job monitoring failed: {event.get('message')}")
