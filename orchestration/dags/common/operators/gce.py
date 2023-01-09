from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from common.config import GCE_ZONE, GCP_PROJECT_ID, SSH_USER, ENV_SHORT_NAME
from common.hooks.gce import GCEHook, SOURCE_IMAGE, STARTUP_SCRIPT
import typing as t


class StartGCEOperator(BaseOperator):
    template_fields = ["instance_name", "instance_type", "preemptible"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        instance_type: str = "n1-standard-1",
        preemptible: bool = True,
        accelerator_types=[],
        *args,
        **kwargs,
    ):
        super(StartGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = instance_name
        self.instance_type = instance_type
        self.preemptible = preemptible
        self.accelerator_types = accelerator_types

    def execute(self, context) -> None:

        source_image = (
            SOURCE_IMAGE["GPU"]
            if len(self.accelerator_types) > 0
            else SOURCE_IMAGE["CPU"]
        )
        startup_script = STARTUP_SCRIPT if len(self.accelerator_types) > 0 else None

        hook = GCEHook(source_image=source_image)
        hook.start_vm(
            self.instance_name,
            self.instance_type,
            preemptible=self.preemptible,
            accelerator_types=self.accelerator_types,
            startup_script=startup_script,
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
        self.instance_name = instance_name

    def execute(self, context):
        hook = GCEHook()
        hook.delete_vm(self.instance_name)


class BaseSSHGCEOperator(BaseOperator):
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

        self.instance_name = instance_name
        self.branch = command
        self.environment = environment
        super(BaseSSHGCEOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = ComputeEngineSSHHook(
            instance_name=self.instance_name,
            zone=GCE_ZONE,
            project_id=GCP_PROJECT_ID,
            use_iap_tunnel=True,
            use_oslogin=False,
            user=SSH_USER,
            gcp_conn_id="google_cloud_default",
        )
        with hook.get_conn() as ssh_client:
            exit_status, agg_stdout, agg_stderr = hook.exec_ssh_client_command(
                ssh_client,
                self.command,
                timeout=1000,
                environment=self.environment,
                get_pty=False,
            )
            if exit_status != 0:
                raise AirflowException(
                    f"SSH operator error: exit status = {exit_status}"
                )

        return agg_stdout


class CloneRepositoryGCEOperator(BaseSSHGCEOperator):
    REPO = "https://github.com/pass-culture/data-gcp.git"

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        command: str,
        environment: t.Dict[str, str] = {},
        *args,
        **kwargs,
    ):
        self.command = self.script(command)
        self.instance_name = instance_name
        self.environment = environment
        super(CloneRepositoryGCEOperator, self).__init__(
            instance_name=self.instance_name,
            command=self.command,
            environment=self.environment,
            *args,
            **kwargs,
        )

    def script(self, branch) -> str:
        return """
        DIR=data-gcp &&
        if [ -d "$DIR" ]; then 
            echo "Update and Checkout repo..." &&
            cd ${DIR} && 
            git checkout master &&
            git pull
        else
            echo "Clone and checkout repo..." &&
            git clone %s &&
            cd ${DIR}
        fi  &&
        git checkout %s &&
        git pull
        """ % (
            self.REPO,
            branch,
        )


class GCloudSSHGCEOperator(BashOperator):
    DEFAULT_EXPORT = {
        "PATH": "/opt/conda/bin:/opt/conda/condabin:+$PATH",
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
    }
    template_fields = ["instance_name", "bash_command"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        base_dir: str,
        command: str,
        export_config: t.Dict[str, str] = {},
        *args,
        **kwargs,
    ):
        self.instance_name = instance_name
        self.command = command
        default_command = "\n".join(
            [
                f"export {key}={value}"
                for key, value in dict(self.DEFAULT_EXPORT, **export_config).items()
            ]
        )
        default_path = f"cd {base_dir}"
        command = "\n".join([default_command, default_path, self.command])
        super(GCloudSSHGCEOperator, self).__init__(
            bash_command=f"gcloud compute ssh {SSH_USER}@{self.instance_name} "
            f"--zone {GCE_ZONE} "
            f"--project {GCP_PROJECT_ID} "
            f"--command '{command}'",
            *args,
            **kwargs,
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
        base_dir: str,
        command: str,
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

        default_path = f"cd {base_dir}"
        self.command = "\n".join([default_command, default_path, command])
        self.instance_name = instance_name
        super(SSHGCEOperator, self).__init__(
            instance_name=self.instance_name,
            command=self.command,
            environment=self.environment,
            *args,
            **kwargs,
        )
