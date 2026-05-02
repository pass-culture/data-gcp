import os
from typing import Literal

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from common.config import ENV_SHORT_NAME, ENVIRONMENT_NAME, LOCAL_ENV
from kubernetes.client import (
    V1Container,
    V1EmptyDirVolumeSource,
    V1Pod,
    V1PodSpec,
    V1Volume,
    V1VolumeMount,
)

_DAGS_REPO_URL = "https://github.com/pass-culture/data-gcp"
_DAGS_REPO_NAME = "data-gcp"
_DAGS_PATH = "orchestration/dags"
_DAGS_VOLUME = "airflow-dags"
_DAGS_MOUNT = "/opt/airflow/dags"

_MS_REPO_URL = "https://github.com/pass-culture/data-gcp"
_MS_REPO_NAME = "data-gcp"
_MS_VOLUME = "microservice-volume"
_MS_MOUNT = "/app"

_REGISTRY = (
    "europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry"
)
_REGISTRY_FOLDER = "data-gcp"
_BASE_IMAGE_TAG = "dev"
_BASE_IMAGE = "py310"
_BASE_IMAGE = f"{_REGISTRY}/{_REGISTRY_FOLDER}/{_BASE_IMAGE}:{_BASE_IMAGE_TAG}"

_DEFAULT_DAGS_BRANCH = os.environ.get(
    "GIT_BRANCH", "master" if ENV_SHORT_NAME != "prod" else "production"
)


# class KubernetesPodOperatorWithGitSync(KubernetesPodOperator):
#     """
#     Extends KubernetesPodOperator with:
#     - A git-clone init container that clones a repo branch into mount_path
#     - Automatic `cd {mount_path} &&` prepended to arguments
#     - full_pod_spec is built at execute time so self.branch is already Jinja-rendered
#     """

#     template_fields = KubernetesPodOperator.template_fields + (
#         "branch",
#         "repo_url",
#         "repo_name",
#         "microservice_path",
#         "mount_path",
#     )

#     def __init__(
#         self,
#         *,
#         repo_url: str,
#         repo_name: str,
#         microservice_path: str,
#         branch: str,
#         mount_path: str = _MS_MOUNT,
#         **kwargs,
#     ):
#         self.branch = branch
#         self.repo_url = repo_url
#         self.repo_name = repo_name
#         self.microservice_path = microservice_path
#         self.mount_path = mount_path

#         if "arguments" in kwargs:
#             kwargs["arguments"] = [f"cd {mount_path} && {kwargs['arguments'][0]}"]

#         # # Inject volume and mount into the main container via KPO native params
#         # kwargs.setdefault("volumes", [])
#         # kwargs["volumes"] = kwargs["volumes"] + [
#         #     V1Volume(name="app-code", empty_dir=V1EmptyDirVolumeSource())
#         # ]
#         # kwargs.setdefault("volume_mounts", [])
#         # kwargs["volume_mounts"] = kwargs["volume_mounts"] + [
#         #     V1VolumeMount(name="app-code", mount_path=mount_path)
#         # ]
#         super().__init__(**kwargs)

#     def execute(self, context):
#         self.full_pod_spec = self._make_pod_spec(
#             repo_url=self.repo_url,
#             repo_name=self.repo_name,
#             microservice_path=self.microservice_path,
#             branch=self.branch,
#             mount_path=self.mount_path,
#         )
#         return super().execute(context)

#     @staticmethod
#     def _make_pod_spec(
#         repo_url: str,
#         repo_name: str,
#         microservice_path: str,
#         branch: str,
#         mount_path: str,
#     ) -> V1Pod:
#         return V1Pod(
#             spec=V1PodSpec(
#                 init_containers=[
#                     V1Container(
#                         name="git-clone",
#                         image="alpine/git",
#                         command=["sh", "-c"],
#                         args=[
#                             f"git clone --depth 1 --branch {branch} {repo_url} /tmp/{repo_name}"
#                             f" && cp -r /tmp/{repo_name}/{microservice_path}/. {mount_path}/"
#                         ],
#                         volume_mounts=[
#                             V1VolumeMount(name="app-code", mount_path=mount_path)
#                         ],
#                     )
#                 ],
#                 containers=[
#                     V1Container(
#                         name="base",
#                         volume_mounts=[
#                             V1VolumeMount(name="app-code", mount_path=mount_path)
#                         ],
#                     )
#                 ],
#                 volumes=[V1Volume(name="app-code", empty_dir=V1EmptyDirVolumeSource())],
#             )
#         )


def _make_job_pod_spec(branch: str, microservice_path: str) -> V1Pod:
    return V1Pod(
        spec=V1PodSpec(
            init_containers=[
                V1Container(
                    name="git-clone",
                    image="alpine/git",
                    command=["sh", "-c"],
                    args=[
                        f"git clone --depth 1 --branch {branch} {_MS_REPO_URL} /tmp/{_MS_REPO_NAME}"
                        f" && cp -r /tmp/{_MS_REPO_NAME}/{microservice_path}/. {_MS_MOUNT}/"
                    ],
                    volume_mounts=[
                        V1VolumeMount(name=_MS_VOLUME, mount_path=_MS_MOUNT)
                    ],
                )
            ],
            containers=[
                V1Container(
                    name="base",
                    volume_mounts=[
                        V1VolumeMount(name=_MS_VOLUME, mount_path=_MS_MOUNT)
                    ],
                )
            ],
            volumes=[V1Volume(name=_MS_VOLUME, empty_dir=V1EmptyDirVolumeSource())],
        )
    )


class EasyKubernetesPodOperator(KubernetesPodOperator):
    """
    Opinionated wrapper around KubernetesPodOperator with two configuration axes:

    orchestration_mode:
      - "celery" (default): task runs on the default celery worker queue; worker pod is
        untouched (fully defined by the Airflow helm chart).
      - "kubernetes": task is routed to the kubernetes queue; the worker pod gets a
        git-sync init container that pulls the DAGs repo at `dags_branch`.
        `dags_branch` must be a concrete string — it is embedded in executor_config at
        __init__ time, before Airflow template rendering.

    runtime_mode:
      - "gitsynced": job pod clones the microservice repo at `branch` and runs the
        entrypoint via `uv run`. `branch` is a template field and is rendered before
        execute(), so Jinja expressions like "{{ params.branch }}" work here.
      - "containerized": job pod uses a fully-built image; no init container. The user
        must provide `image=`.

    Parameters handled implicitly (must not appear in DAG code):
      git-sync init containers, volume wiring, executor_config / queue assignment,
      base image selection, uv entrypoint construction.
    """

    template_fields = KubernetesPodOperator.template_fields + ("branch", "dags_branch")

    def __init__(
        self,
        *,
        runtime_mode: Literal["gitsynced", "containerized"],
        orchestration_mode: Literal["celery", "kubernetes"] = "celery",
        dags_branch: str = _DEFAULT_DAGS_BRANCH,
        branch: str | None = None,
        microservice_path: str | None = None,
        namespace: str = f"airflow-{ENVIRONMENT_NAME}",
        service_account_name: str = "airflow-worker",
        in_cluster: bool | None = None,
        get_logs: bool = True,
        is_delete_operator_pod: bool = True,
        on_finish_action: str = "delete_pod",
        image_pull_policy: str | None = None,
        kubernetes_conn_id: str | None = None,
        **kwargs,
    ):
        if runtime_mode == "gitsynced":
            if branch is None:
                raise ValueError("branch is required for runtime_mode='gitsynced'")
            if microservice_path is None:
                raise ValueError(
                    "microservice_path is required for runtime_mode='gitsynced'"
                )

        self.runtime_mode = runtime_mode
        self.orchestration_mode = orchestration_mode
        self.dags_branch = dags_branch
        self.branch = branch
        self.microservice_path = microservice_path

        if in_cluster is None:
            in_cluster = not bool(LOCAL_ENV)
        if image_pull_policy is None:
            image_pull_policy = "Always" if ENV_SHORT_NAME == "dev" else "IfNotPresent"
        if kubernetes_conn_id is None:
            kubernetes_conn_id = "kubernetes_default" if LOCAL_ENV else None

        if runtime_mode == "gitsynced":
            kwargs.setdefault("image", _BASE_IMAGE)
            if "arguments" in kwargs:
                kwargs["arguments"] = [
                    f"cd {_MS_MOUNT} && uv run {kwargs['arguments'][0]}"
                ]
            kwargs["cmds"] = ["sh", "-c"]

        if orchestration_mode == "kubernetes":
            kwargs["queue"] = "kubernetes"
            kwargs["executor_config"] = {
                "pod_override": _make_worker_pod_spec(dags_branch)
            }

        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            in_cluster=in_cluster,
            get_logs=get_logs,
            is_delete_operator_pod=is_delete_operator_pod,
            on_finish_action=on_finish_action,
            image_pull_policy=image_pull_policy,
            kubernetes_conn_id=kubernetes_conn_id,
            **kwargs,
        )

    def execute(self, context):
        if self.runtime_mode == "gitsynced":
            self.full_pod_spec = _make_job_pod_spec(self.branch, self.microservice_path)
        return super().execute(context)


def _make_worker_pod_spec(dags_branch: str) -> V1Pod:
    return V1Pod(
        spec=V1PodSpec(
            init_containers=[
                V1Container(
                    name="git-sync",
                    image="alpine/git",
                    command=["sh", "-c"],
                    args=[
                        f"git clone --depth 1 --branch {dags_branch} {_DAGS_REPO_URL} /tmp/{_DAGS_REPO_NAME}"
                        f" && cp -r /tmp/{_DAGS_REPO_NAME}/{_DAGS_PATH}/. {_DAGS_MOUNT}/"
                    ],
                    volume_mounts=[
                        V1VolumeMount(name=_DAGS_VOLUME, mount_path=_DAGS_MOUNT)
                    ],
                )
            ],
            containers=[
                V1Container(
                    name="base",
                    volume_mounts=[
                        V1VolumeMount(name=_DAGS_VOLUME, mount_path=_DAGS_MOUNT)
                    ],
                )
            ],
        )
    )
