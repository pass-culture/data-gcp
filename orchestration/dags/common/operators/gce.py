from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.configuration import conf
from time import sleep
from paramiko.ssh_exception import SSHException
from common.config import (
    GCE_ZONE,
    GCP_PROJECT_ID,
    GCE_BASE_PREFIX,
    SSH_USER,
    ENV_SHORT_NAME,
)
from common.hooks.gce import GCEHook, GPUImage, CPUImage
import typing as t
from base64 import b64encode


class StartGCEOperator(BaseOperator):
    template_fields = ["instance_name", "instance_type", "preemptible"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        instance_type: str = "n1-standard-1",
        preemptible: bool = True,
        accelerator_types=[],
        source_image_type=None,
        labels={},
        *args,
        **kwargs,
    ):
        super(StartGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = f"{GCE_BASE_PREFIX}-{instance_name}"
        self.instance_type = instance_type
        self.preemptible = preemptible
        self.accelerator_types = accelerator_types
        if source_image_type is None:
            source_image_type = (
                GPUImage() if len(self.accelerator_types) > 0 else CPUImage()
            )

        self.source_image_type = source_image_type
        self.labels = labels

    def execute(self, context) -> None:
        hook = GCEHook(source_image_type=self.source_image_type)
        hook.start_vm(
            self.instance_name,
            self.instance_type,
            preemptible=self.preemptible,
            accelerator_types=self.accelerator_types,
            labels=self.labels,
        )


class CleanGCEOperator(BaseOperator):
    template_fields = ["timeout_in_minutes"]

    @apply_defaults
    def __init__(
        self,
        timeout_in_minutes: int,
        *args,
        **kwargs,
    ):
        super(CleanGCEOperator, self).__init__(*args, **kwargs)
        self.timeout_in_minutes = timeout_in_minutes

    def execute(self, context) -> None:
        hook = GCEHook()
        hook.delete_instances(timeout_in_minutes=self.timeout_in_minutes)


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
        *args,
        **kwargs,
    ):
        self.command = self.script(command, python_version)
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

    def script(self, branch, python_version) -> str:
        return """
        export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH
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
        *args,
        **kwargs,
    ):
        self.environment = dict(self.DEFAULT_EXPORT, **environment)
        default_command = "\n".join(
            [
                f"export {key}={value}"
                for key, value in dict(self.DEFAULT_EXPORT, **environment).items()
            ]
        )
        activate_env = "conda init zsh && source ~/.zshrc && conda activate data-gcp"
        default_path = f"cd {base_dir}" if base_dir is not None else ""
        self.command = "\n".join([default_command, activate_env, default_path, command])
        self.instance_name = instance_name
        super(SSHGCEOperator, self).__init__(
            instance_name=self.instance_name,
            command=self.command,
            environment=self.environment,
            *args,
            **kwargs,
        )
