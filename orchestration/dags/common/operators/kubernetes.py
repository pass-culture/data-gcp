from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import (
    V1Container,
    V1EmptyDirVolumeSource,
    V1Pod,
    V1PodSpec,
    V1Volume,
    V1VolumeMount,
)


class KubernetesPodOperatorWithGitSync(KubernetesPodOperator):
    """
    Extends KubernetesPodOperator with:
    - A git-clone init container that clones a repo branch into mount_path
    - Automatic `cd {mount_path} &&` prepended to arguments
    - full_pod_spec is built at execute time so self.branch is already Jinja-rendered
    """

    template_fields = KubernetesPodOperator.template_fields + (
        "branch",
        "repo_url",
        "repo_name",
        "microservice_path",
        "mount_path",
    )

    def __init__(
        self,
        *,
        repo_url: str,
        repo_name: str,
        microservice_path: str,
        branch: str,
        mount_path: str = "/app",
        **kwargs,
    ):
        self.branch = branch
        self.repo_url = repo_url
        self.repo_name = repo_name
        self.microservice_path = microservice_path
        self.mount_path = mount_path

        if "arguments" in kwargs:
            kwargs["arguments"] = [f"cd {mount_path} && {kwargs['arguments'][0]}"]

        # # Inject volume and mount into the main container via KPO native params
        # kwargs.setdefault("volumes", [])
        # kwargs["volumes"] = kwargs["volumes"] + [
        #     V1Volume(name="app-code", empty_dir=V1EmptyDirVolumeSource())
        # ]
        # kwargs.setdefault("volume_mounts", [])
        # kwargs["volume_mounts"] = kwargs["volume_mounts"] + [
        #     V1VolumeMount(name="app-code", mount_path=mount_path)
        # ]
        super().__init__(**kwargs)

    def execute(self, context):
        self.full_pod_spec = self._make_pod_spec(
            repo_url=self.repo_url,
            repo_name=self.repo_name,
            microservice_path=self.microservice_path,
            branch=self.branch,
            mount_path=self.mount_path,
        )
        return super().execute(context)

    @staticmethod
    def _make_pod_spec(
        repo_url: str,
        repo_name: str,
        microservice_path: str,
        branch: str,
        mount_path: str,
    ) -> V1Pod:
        return V1Pod(
            spec=V1PodSpec(
                init_containers=[
                    V1Container(
                        name="git-clone",
                        image="alpine/git",
                        command=["sh", "-c"],
                        args=[
                            f"git clone --depth 1 --branch {branch} {repo_url} /tmp/{repo_name}"
                            f" && cp -r /tmp/{repo_name}/{microservice_path}/. {mount_path}/"
                        ],
                        volume_mounts=[
                            V1VolumeMount(name="app-code", mount_path=mount_path)
                        ],
                    )
                ],
                containers=[
                    V1Container(
                        name="base",
                        volume_mounts=[
                            V1VolumeMount(name="app-code", mount_path=mount_path)
                        ],
                    )
                ],
                volumes=[V1Volume(name="app-code", empty_dir=V1EmptyDirVolumeSource())],
            )
        )
