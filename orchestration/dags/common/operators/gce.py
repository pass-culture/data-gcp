from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from common.config import GCE_ZONE, GCP_PROJECT_ID, SSH_USER, ENV_SHORT_NAME
from common.hooks.gce import GCEHook
import typing as t


class StartGCEOperator(BaseOperator):
    template_fields = ["instance_name", "machine_type", "preemptible"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        machine_type: str = "n1-standard-1",
        preemptible: bool = True,
        *args,
        **kwargs,
    ):
        super(StartGCEOperator, self).__init__(*args, **kwargs)
        self.instance_name = instance_name
        self.machine_type = machine_type
        self.preemptible = preemptible

    def execute(self, context) -> None:
        hook = GCEHook()
        hook.start_vm(
            self.instance_name,
            self.machine_type,
            preemptible=self.preemptible,
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


class CloneRepositoryGCEOperator(SSHOperator):
    REPO = "https://github.com/pass-culture/data-gcp.git"
    template_fields = ["instance_name", "command"]

    @apply_defaults
    def __init__(
        self,
        instance_name: str,
        branch: str,
        *args,
        **kwargs,
    ):
        self.instance_name = instance_name
        self.branch = branch
        hook = ComputeEngineSSHHook(
            instance_name=self.instance_name,
            zone=GCE_ZONE,
            project_id=GCP_PROJECT_ID,
            use_iap_tunnel=True,
            use_oslogin=False,
            user=SSH_USER,
            gcp_conn_id="google_cloud_default",
        )
        super(CloneRepositoryGCEOperator, self).__init__(
            ssh_hook=hook, command=self.script, *args, **kwargs
        )

    @property
    def script(self) -> str:
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
            self.branch,
        )


class SSHGCEOperator(BashOperator):
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
        super(SSHGCEOperator, self).__init__(
            bash_command=f"gcloud compute ssh {SSH_USER}@{self.instance_name} "
            f"--zone {GCE_ZONE} "
            f"--project {GCP_PROJECT_ID} "
            f"--command '{command}'",
            *args,
            **kwargs,
        )
