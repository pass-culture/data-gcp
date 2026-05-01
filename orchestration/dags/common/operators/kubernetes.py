import re
from abc import ABC, abstractmethod
from typing import Literal

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from common.config import ENV_SHORT_NAME, ENVIRONMENT_NAME, GCP_PROJECT_ID, LOCAL_ENV
from kubernetes.client import (
    V1Container,
    V1EmptyDirVolumeSource,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
)

_DOCKER_REGISTRY = (
    "europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry"
)
_DATA_GCP_REPO_URL = "https://github.com/pass-culture/data-gcp"
_DATA_GCP_REPO_NAME = "data-gcp"
_DATA_GCP_DAGS_PATH = "orchestration/dags"
_DEFAULT_BRANCH = "production" if ENV_SHORT_NAME == "prod" else "master"


def _make_git_sync_pod_spec(
    repo_url: str,
    repo_name: str,
    microservice_path: str,
    branch: str,
    mount_path: str,
    mount_name: str = "app-code",
    main_container_name: str = "base",
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
                        f" && rm -rf /tmp/{repo_name}"
                    ],
                    volume_mounts=[
                        V1VolumeMount(name=mount_name, mount_path=mount_path)
                    ],
                )
            ],
            containers=[
                V1Container(
                    name=main_container_name,
                    volume_mounts=[
                        V1VolumeMount(name=mount_name, mount_path=mount_path)
                    ],
                )
            ],
            volumes=[V1Volume(name=mount_name, empty_dir=V1EmptyDirVolumeSource())],
        )
    )


def _make_dag_sync_pod_override(
    repo_url: str,
    repo_name: str,
    branch: str,
    dags_path: str = _DATA_GCP_DAGS_PATH,
    mount_path: str = "/opt/airflow/dags",
    mount_name: str = "airflow-dags",
) -> V1Pod:
    """
    Pod override for executor_config — injects only the git-clone init container.
    No volumes or containers are redefined: the base worker pod already owns the
    airflow-dags volume and the main container mount.
    """
    return V1Pod(
        spec=V1PodSpec(
            init_containers=[
                V1Container(
                    name="git-clone",
                    image="alpine/git",
                    command=["sh", "-c"],
                    args=[
                        f"git clone --depth 1 --branch {branch} {repo_url} /tmp/{repo_name}"
                        f" && cp -r /tmp/{repo_name}/{dags_path}/. {mount_path}/"
                        f" && rm -rf /tmp/{repo_name}"
                    ],
                    volume_mounts=[
                        V1VolumeMount(name=mount_name, mount_path=mount_path)
                    ],
                )
            ],
        )
    )


def _sanitize_pod_name(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")[:253]


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

        super().__init__(**kwargs)

    def execute(self, context):
        self.full_pod_spec = _make_git_sync_pod_spec(
            repo_url=self.repo_url,
            repo_name=self.repo_name,
            microservice_path=self.microservice_path,
            branch=self.branch,
            mount_path=self.mount_path,
        )
        return super().execute(context)


# ---------------------------------------------------------------------------
# Sync strategies — determine how (and whether) code is injected at runtime.
# Applied in EasyKubernetesPodOperator.execute() after Jinja renders `branch`.
# ---------------------------------------------------------------------------


class SyncStrategy(ABC):
    @abstractmethod
    def apply(self, operator: KubernetesPodOperator) -> None: ...


class NoSyncStrategy(SyncStrategy):
    """No-op — fully containerized images carry their own code."""

    def apply(self, _: KubernetesPodOperator) -> None:
        pass


class K8sSyncStrategy(SyncStrategy):
    """Syncs microservice code into the KPO-launched pod via full_pod_spec."""

    def apply(self, operator: KubernetesPodOperator) -> None:
        operator.full_pod_spec = _make_git_sync_pod_spec(
            repo_url=operator.repo_url,
            repo_name=operator.repo_name,
            microservice_path=operator.microservice_path,
            branch=operator.branch,
            mount_path=operator.mount_path,
        )


class CelerySyncStrategy(SyncStrategy):
    """
    Injects a git-clone init container into the Celery worker pod via executor_config.
    Skipped when running on the environment's default branch (code already deployed).
    """

    def apply(self, operator: KubernetesPodOperator) -> None:
        if operator.branch != _DEFAULT_BRANCH:
            operator.executor_config = {
                "pod_override": _make_git_sync_pod_spec(
                    repo_url=operator.repo_url,
                    repo_name=operator.repo_name,
                    microservice_path=operator.microservice_path,
                    branch=operator.branch,
                    mount_path=operator.mount_path,
                )
            }


def _select_strategy(
    executor: Literal["k8s", "celery"],
    image_type: Literal["base", "microservice"],
) -> SyncStrategy:
    if image_type == "microservice":
        return NoSyncStrategy()
    return K8sSyncStrategy() if executor == "k8s" else CelerySyncStrategy()


# ---------------------------------------------------------------------------
# EasyKubernetesPodOperator — Adapter + Strategy
# ---------------------------------------------------------------------------


class EasyKubernetesPodOperator(KubernetesPodOperator):
    """
    Adapter over KubernetesPodOperator with project-wide boilerplate pre-filled
    and a Strategy-based sync injection mechanism.

    image_type="base" (default)
        Base image containing no code or pre-installed libraries. A git-clone
        init container is injected at runtime to fetch the microservice code.

    image_type="microservice"
        Fully self-contained image (code + deps baked in). Runs directly,
        no git sync, no `cd` prepend.

    executor="k8s" (default)
        Routes to queue="kubernetes". Sync injected via full_pod_spec init
        container on the KPO pod.

    executor="celery"
        No queue override. Sync injected via executor_config pod_override on
        the Celery worker pod — only when branch differs from the environment's
        default branch (master / production).

    Override contract
    -----------------
    All defaults yield to explicit kwargs (setdefault throughout).
    - image=        overrides image_name + image_tag construction
    - container_resources=  overrides cpu_*/memory_* params
    - env_vars=     replaces all default env vars entirely
    - extra_env_vars=  adds/replaces individual keys within the defaults
    - sync_strategy=   bypasses auto-selection (executor × image_type)
    - prepend_cd=False  opts out of `cd {mount_path} &&` even for base images
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
        executor: Literal["k8s", "celery"] = "k8s",
        image_type: Literal["base", "microservice"] = "base",
        microservice_path: str | None = None,
        image_name: str,
        image_tag: str = ENV_SHORT_NAME,
        branch: str = _DEFAULT_BRANCH,
        repo_url: str = _DATA_GCP_REPO_URL,
        repo_name: str = _DATA_GCP_REPO_NAME,
        mount_path: str = "/app",
        container_resources: V1ResourceRequirements | None = None,
        extra_env_vars: dict | None = None,
        sync_strategy: SyncStrategy | None = None,
        prepend_cd: bool = True,
        **kwargs,
    ):
        if image_type == "base" and microservice_path is None:
            raise ValueError("microservice_path is required when image_type='base'")

        self.branch = branch
        self.repo_url = repo_url
        self.repo_name = repo_name
        self.microservice_path = microservice_path
        self.mount_path = mount_path
        self._executor = executor
        self._image_type = image_type
        self._sync_strategy_override = sync_strategy

        if image_type == "base" and prepend_cd and "arguments" in kwargs:
            kwargs["arguments"] = [f"cd {mount_path} && {kwargs['arguments'][0]}"]

        kwargs.setdefault(
            "image",
            f"{_DOCKER_REGISTRY}/data-gcp/{image_name}:{image_tag}",
        )
        kwargs.setdefault("namespace", f"airflow-{ENVIRONMENT_NAME}")
        kwargs.setdefault(
            "container_resources",
            container_resources
            or V1ResourceRequirements(
                requests={"cpu": "0.2", "memory": "1Gi"},
                limits={"cpu": "0.5", "memory": "1Gi"},
            ),
        )

        default_env = {
            "GCP_PROJECT_ID": GCP_PROJECT_ID,
            "ENV_SHORT_NAME": ENV_SHORT_NAME,
            "GIT_BRANCH": branch,
            "UV_CACHE_DIR": "/app/.cache/uv",
        }
        if extra_env_vars:
            default_env.update(extra_env_vars)
        kwargs.setdefault("env_vars", default_env)

        kwargs.setdefault("in_cluster", not LOCAL_ENV)
        kwargs.setdefault("get_logs", True)
        kwargs.setdefault("is_delete_operator_pod", True)
        kwargs.setdefault("on_finish_action", "delete_pod")
        kwargs.setdefault(
            "image_pull_policy",
            "Always" if ENV_SHORT_NAME == "dev" else "IfNotPresent",
        )
        if executor == "k8s":
            kwargs.setdefault("queue", "kubernetes")
            if (
                image_type == "base"
                and branch != _DEFAULT_BRANCH
                and "{{" not in branch
            ):
                kwargs.setdefault(
                    "executor_config",
                    {
                        "pod_override": _make_dag_sync_pod_override(
                            repo_url=repo_url,
                            repo_name=repo_name,
                            branch=branch,
                        )
                    },
                )
        kwargs.setdefault("service_account_name", "airflow-worker")
        kwargs.setdefault(
            "kubernetes_conn_id",
            "kubernetes_default" if LOCAL_ENV else None,
        )

        if "name" not in kwargs:
            kwargs["name"] = _sanitize_pod_name(kwargs.get("task_id", ""))

        super().__init__(**kwargs)

    def execute(self, context):
        strategy = self._sync_strategy_override or _select_strategy(
            self._executor, self._image_type
        )
        strategy.apply(self)
        return super().execute(context)
