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
)
from common.hooks.gce import GCEHook
from common.hooks.image import MACHINE_TYPE
from common.hooks.network import BASE_NETWORK_LIST, GKE_NETWORK_LIST
from paramiko.ssh_exception import SSHException

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.utils.decorators import apply_defaults

UV_VERSION = "0.5.2"


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
        hook.delete_vm(self.instance_name)


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
        self.command = (
            self.clone_and_init_with_uv(command, python_version)
            if self.use_uv
            else self.clone_and_init_with_conda(command, python_version)
        )
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

    def clone_and_init_with_conda(self, branch, python_version) -> str:
        return f"""
        export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH
        python -m pip install --upgrade --user urllib3
        conda create --name data-gcp python={python_version} -y -q
        conda init zsh
        source ~/.zshrc
        conda activate data-gcp
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
        "installer",
        "base_dir",
    ]
    DEFAULT_EXPORT = {
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "ENVIRONMENT_NAME": ENVIRONMENT_NAME,
    }
    CONDA_EXPORT = {
        **DEFAULT_EXPORT,
        "PATH": "/opt/conda/bin:/opt/conda/condabin:+$PATH",
    }
    UV_EXPORT = {**DEFAULT_EXPORT, "PATH": "$HOME/.local/bin:$PATH"}

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        base_dir: str = None,
        environment: t.Dict[str, str] = {},
        installer: str = "uv",
        *args,
        **kwargs,
    ):
        self.base_dir = base_dir
        self.installer = installer
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

    def execute(self, context):
        environment = (
            dict(self.CONDA_EXPORT, **self.environment)
            if self.installer == "conda"
            else dict(self.UV_EXPORT, **self.environment)
        )
        commands_list = []
        commands_list.append(
            "\n".join([f"export {key}={value}" for key, value in environment.items()])
        )

        if self.base_dir is not None:
            commands_list.append(f"cd ~/{self.base_dir}")

        if self.installer == "conda":
            commands_list.append(
                "conda init zsh && source ~/.zshrc && conda activate data-gcp"
            )
        elif self.installer == "uv":
            commands_list.append("source .venv/bin/activate")
        else:
            commands_list.append("echo no virtual environment activation")

        commands_list.append(self.command)

        final_command = "\n".join(commands_list)
        self.command = final_command
        return super().execute(context)


class InstallDependenciesOperator(SSHGCEOperator):
    REPO = "https://github.com/pass-culture/data-gcp.git"
    template_fields = set(
        [
            "installer",
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
        installer: str = "uv",  # Default to 'uv'
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
        self.installer = installer
        self.branch = branch
        self.base_dir = base_dir
        # Call the parent class constructor but do not pass the command yet
        super(InstallDependenciesOperator, self).__init__(
            instance_name=self.instance_name,
            command="",  # Placeholder command
            environment=self.environment,
            base_dir=self.base_dir,  # Pass base_dir to parent class
            installer=self.installer,
            *args,
            **kwargs,
        )

    def execute(self, context):
        if self.installer not in ["uv", "conda"]:
            raise ValueError(f"Invalid installer: {self.installer}")
        # The templates have been rendered; we can construct the command
        command = self.make_install_command(
            self.installer, self.requirement_file, self.branch, self.base_dir
        )
        # Use the command in the parent SSHGCEOperator to execute on the remote instance
        self.command = command
        return super(InstallDependenciesOperator, self).execute(context)

    def make_install_command(
        self,
        installer: str,
        requirement_file: str,
        branch: str,
        base_dir: str = "data-gcp",
    ) -> str:
        """
        Construct the command to clone the repo and install dependencies based on the installer.
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

        if installer == "uv":
            install_command = f"""
                curl -LsSf https://astral.sh/uv/{UV_VERSION}/install.sh | sh &&
                cd {base_dir} &&
                uv venv --python {self.python_version} &&
                source .venv/bin/activate &&
                uv pip sync {requirement_file}
            """
        elif installer == "conda":
            install_command = f"""
                export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH &&
                python -m pip install --upgrade --user urllib3 &&
                conda create --name data-gcp python={self.python_version} -y -q &&
                conda init zsh &&
                cd {base_dir} &&
                source ~/.zshrc &&
                conda activate data-gcp &&
                pip install -r {requirement_file} --user
            """
        else:
            raise ValueError(f"Invalid installer: {installer}. Choose 'uv' or 'conda'.")
        # Combine the git clone and installation commands
        return f"""
            {clone_command}
            {install_command}
        """
