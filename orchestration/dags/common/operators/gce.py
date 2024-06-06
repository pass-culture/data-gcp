import typing as t
from base64 import b64encode
from time import sleep

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.utils.decorators import apply_defaults
from common.config import (
    ENV_SHORT_NAME,
    GCE_BASE_PREFIX,
    GCE_ZONE,
    GCP_PROJECT_ID,
    SSH_USER,
)
from common.hooks.gce import GCEHook
from common.hooks.image import MACHINE_TYPE
from paramiko.ssh_exception import SSHException


class StartGCEOperator(BaseOperator):
    template_fields = [
        "instance_name",
        "instance_type",
        "preemptible",
        "accelerator_types",
        "gpu_count",
        "source_image_type",
        "labels",
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
        labels={},
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
        self.labels = labels

    def execute(self, context) -> None:
        if self.source_image_type is None:
            if len(self.accelerator_types) > 0 or self.gpu_count > 0:
                image_type = MACHINE_TYPE["gpu"]
            else:
                image_type = MACHINE_TYPE["cpu"]
        else:
            image_type = MACHINE_TYPE[self.source_image_type]
        hook = GCEHook(source_image_type=image_type)
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
        use_pyenv: bool = False,
        *args,
        **kwargs,
    ):
        self.command = (
            self.clone_and_init_with_conda(command, python_version)
            if use_pyenv is False
            else self.clone_and_init_with_pyenv(command)
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

    def clone_and_init_with_pyenv(self, branch) -> str:
        return """
        python -m pip install --upgrade --user urllib3

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
        make prerequisites_on_debian_vm
        """ % (
            branch,
            self.REPO,
            branch,
        )


class SSHGCEOperator(BaseSSHGCEOperator):
    DEFAULT_EXPORT = {
        "PATH": "/opt/conda/bin:/opt/conda/condabin:+$PATH",
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
    }

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        base_dir: str = None,
        environment: t.Dict[str, str] = {},
        use_pyenv: bool = False,
        *args,
        **kwargs,
    ):
        self.environment = dict(self.DEFAULT_EXPORT, **environment)
        commands_list = []

        # Default export
        commands_list.append(
            "\n".join(
                [
                    f"export {key}={value}"
                    for key, value in dict(self.DEFAULT_EXPORT, **environment).items()
                ]
            )
        )

        # Conda activate if required
        if not use_pyenv:
            commands_list.append(
                "conda init zsh && source ~/.zshrc && conda activate data-gcp"
            )
        else:
            commands_list.append("source ~/.profile")

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
