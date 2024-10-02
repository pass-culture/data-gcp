import typing as t
from base64 import b64encode
from time import sleep

from common.config import (
    ENV_SHORT_NAME,
    GCE_BASE_PREFIX,
    GCE_ZONE,
    GCP_PROJECT_ID,
    INSTALL_TYPES,
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


class StartGCEOperator(BaseOperator):
    template_fields = [
        "instance_name",
        "instance_type",
        "preemptible",
        "accelerator_types",
        "gpu_count",
        "source_image_type",
        "disk_size_gb",
        "labels",
        "use_gke_network",
    ]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        instance_type: str = "n1-standard-1",
        preemptible: bool = True,
        accelerator_types=[],
        gpu_count: int = 0,
        source_image_type: str = None,
        disk_size_gb: str = "100",
        labels={},
        use_gke_network: bool = False,
        *args,
        **kwargs,
    ):
        super(StartGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.instance_type = instance_type
        self.preemptible = preemptible
        self.accelerator_types = accelerator_types
        self.gpu_count = gpu_count
        self.source_image_type = source_image_type
        self.disk_size_gb = disk_size_gb
        self.labels = labels
        self.use_gke_network = use_gke_network

    def execute(self, context) -> None:
        if self.source_image_type is None:
            if len(self.accelerator_types) > 0 or self.gpu_count > 0:
                image_type = MACHINE_TYPE["gpu"]
            else:
                image_type = MACHINE_TYPE["cpu"]
        else:
            image_type = MACHINE_TYPE[self.source_image_type]

        gce_networks = (
            GKE_NETWORK_LIST if self.use_gke_network is True else BASE_NETWORK_LIST
        )

        hook = GCEHook(
            source_image_type=image_type,
            disk_size_gb=self.disk_size_gb,
            gce_networks=gce_networks,
        )
        hook.start_vm(
            self.instance_name,
            self.instance_type,
            preemptible=self.preemptible,
            accelerator_types=self.accelerator_types,
            labels=self.labels,
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
    template_fields = ["instance_name", "command", "environment"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        environment: t.Dict[str, str] = {},
        *args,
        **kwargs,
    ):
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.command = command
        self.environment = environment
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
                    lines_result = agg_stdout.decode("utf-8").split("\n")
                    if len(lines_result[-1]) > 0:
                        result = lines_result[-1]
                    else:
                        result = lines_result[-2]
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
            zone=GCE_ZONE,
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
        use_uv: bool = False,
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
        return """
        curl -LsSf https://astral.sh/uv/install.sh | sh
        uv venv --python %s
        DIR=data-gcp &&
        if [ -d "$DIR" ]; then
            echo "Update and Checkout repo..." &&
            cd ${DIR} &&
            git fetch --all &&
            git reset --hard origin/%s
        else
            echo "Clone and checkout repo..." &&
            git clone %s &&
            cd ${DIR} &&
            git checkout %s
        fi
        """ % (
            python_version,
            branch,
            self.REPO,
            branch,
        )

    def clone_and_init_with_conda(self, branch, python_version) -> str:
        return """
        export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH
        python -m pip install --upgrade --user urllib3
        conda create --name data-gcp python=%s -y -q
        conda init zsh
        source ~/.zshrc
        conda activate data-gcp

        DIR=data-gcp &&
        if [ -d "$DIR" ]; then
            echo "Update and Checkout repo..." &&
            cd ${DIR} &&
            git fetch --all &&
            git reset --hard origin/%s
        else
            echo "Clone and checkout repo..." &&
            git clone %s &&
            cd ${DIR} &&
            git checkout %s
        fi
        """ % (
            python_version,
            branch,
            self.REPO,
            branch,
        )


class SSHGCEOperator(BaseSSHGCEOperator):
    DEFAULT_EXPORT = {
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
    }
    CONDA_EXPORT = {
        **DEFAULT_EXPORT,
        "PATH": "/opt/conda/bin:/opt/conda/condabin:+$PATH",
    }

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        base_dir: str = None,
        environment: t.Dict[str, str] = {},
        installer: str = "conda",
        *args,
        **kwargs,
    ):
        self.environment = (
            dict(self.CONDA_EXPORT, **environment)
            if installer == "conda"
            else dict(self.DEFAULT_EXPORT, **environment)
        )

        commands_list = []

        # Default export
        commands_list.append(
            "\n".join(
                [f"export {key}={value}" for key, value in self.environment.items()]
            )
        )
        # Conda activate if required
        if installer == "conda":
            # Init conda
            commands_list.append(
                "conda init zsh && source ~/.zshrc && conda activate data-gcp"
            )
        elif installer == "uv":
            commands_list.append("source .venv/bin/activate")

        # Default path
        if base_dir is not None:
            commands_list.append(f"cd {base_dir}")

        # Command
        commands_list.append(command)

        self.command = "\n".join(commands_list)
        self.instance_name = instance_name
        super(SSHGCEOperator, self).__init__(
            instance_name=self.instance_name,
            command=self.command,
            environment=self.environment,
            *args,
            **kwargs,
        )


class InstallDependenciesOperator(SSHGCEOperator):
    REPO = "https://github.com/pass-culture/data-gcp.git"
    template_fields = (
        "installer",
        "requirement_file",
        "branch",
        "instance_name",
        "base_dir",
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
        ROOT_REPO_DIR = "data-gcp"

        # Git clone command
        clone_command = f"""
            DIR={ROOT_REPO_DIR} &&
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
            cd ..
        """

        if installer == "uv":
            install_command = f"""
                curl -LsSf https://astral.sh/uv/install.sh | sh &&
                source $HOME/.cargo/env &&
                uv venv --python {self.python_version} &&
                source .venv/bin/activate &&
                cd {base_dir} &&
                uv pip sync {requirement_file}
            """
        elif installer == "conda":
            install_command = f"""
                export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH &&
                python -m pip install --upgrade --user urllib3 &&
                conda create --name data-gcp python={self.python_version} -y -q &&
                conda init zsh &&
                source ~/.zshrc &&
                conda activate data-gcp &&
                cd {base_dir} &&
                pip install -r {requirement_file} --user
            """
        else:
            raise ValueError(f"Invalid installer: {installer}. Choose 'uv' or 'conda'.")

        # Combine the git clone and installation commands
        return f"""
            {clone_command}
            {install_command}
        """


class QuickInstallOperator(SSHGCEOperator):
    REPO = "https://github.com/pass-culture/data-gcp.git"
    template_fields = (
        "installer",
        "branch",
        "instance_name",
        "install_type",
        "python_version",
    )

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        installer: str = "uv",  # Default to 'uv'
        branch: str = "main",  # Branch for repo
        install_type: str = "simple",
        environment: t.Dict[str, str] = {},
        python_version: str = "3.10",
        *args,
        **kwargs,
    ):
        self.instance_name = instance_name
        self.environment = environment
        self.python_version = python_version
        self.installer = installer
        self.branch = branch
        self.install_type = install_type

        # Call the parent class constructor but do not pass the command yet
        super(QuickInstallOperator, self).__init__(
            instance_name=self.instance_name,
            command="",  # Placeholder command
            environment=self.environment,
            base_dir="",  # Placeholder base_dir
            installer=self.installer,
            *args,
            **kwargs,
        )

    def execute(self, context):
        if self.installer not in ["uv", "conda"]:
            raise ValueError(f"Invalid installer: {self.installer}")
        if self.install_type not in ["simple", "engineering", "science", "analytics"]:
            raise ValueError(f"Invalid install_type: {self.install_type}")
        # The templates have been rendered; we can construct the command

        command = self.make_install_command(
            self.installer, INSTALL_TYPES[self.install_type], self.branch
        )

        # Use the command in the parent SSHGCEOperator to execute on the remote instance
        self.command = command
        return super(QuickInstallOperator, self).execute(context)

    def make_install_command(
        self,
        installer: str,
        make_install: str,
        branch: str,
    ) -> str:
        """
        Construct the command to clone the repo and install dependencies based on the installer.
        """
        # Define the directory where the repo will be cloned
        ROOT_REPO_DIR = "data-gcp"

        # Git clone command
        clone_command = f"""
            DIR={ROOT_REPO_DIR} &&
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
        """

        if installer == "uv":
            quick_install_command = f"""
                curl -LsSf https://astral.sh/uv/install.sh | sh &&
                source $HOME/.cargo/env &&
                uv venv --python {self.python_version} &&
                source .venv/bin/activate &&
                NO_GCP_INIT=1 make {make_install} PYTHON_VENV_VERSION={self.python_version}
            """
        elif installer == "conda":
            quick_install_command = f"""
                export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH &&
                python -m pip install --upgrade --user urllib3 &&
                conda create --name data-gcp python={self.python_version} -y -q &&
                conda init zsh &&
                source ~/.zshrc &&
                conda activate data-gcp &&
                NO_GCP_INIT=1 make {make_install} PYTHON_VENV_VERSION={self.python_version}
            """
        else:
            raise ValueError(f"Invalid installer: {installer}. Choose 'uv' or 'conda'.")

        # Combine the git clone and installation commands
        return f"""
            {clone_command}
            {quick_install_command}
        """
