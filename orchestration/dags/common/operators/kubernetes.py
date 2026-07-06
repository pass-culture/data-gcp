import os
import re
from typing import Literal

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from common.config import AIRFLOW_NAMESPACE, ENV_SHORT_NAME, GCP_PROJECT_ID, LOCAL_ENV
from kubernetes.client import (
    V1Container,
    V1EmptyDirVolumeSource,
    V1Pod,
    V1PodSecurityContext,
    V1PodSpec,
    V1ResourceRequirements,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)

_DAGS_REPO_URL = "https://github.com/pass-culture/data-gcp"
_DAGS_REPO_NAME = "data-gcp"
_DAGS_PATH = "orchestration/dags"
_DAGS_VOLUME = "airflow-dags"
_DAGS_MOUNT = "/opt/airflow/dags"
_DAGS_SPARSE_PATHS = [
    "orchestration/dags/common",
    "orchestration/dags/jobs",
]  # sparse-clone only the dags folder for efficiency

_MS_REPO_URL = "https://github.com/pass-culture/data-gcp"
_MS_REPO_NAME = "data-gcp"
_MS_VOLUME = "microservice-volume"
_MS_MOUNT = "/app"

_REGISTRY = (
    "europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry"
)
_REGISTRY_FOLDER = "data-gcp"
_BASE_PYTHON_IMAGE_NAME = "py312"
_CELERY_WORKER_IMAGE_NAME = "airflow"

_DEFAULT_DAGS_BRANCH = "master" if ENV_SHORT_NAME != "prod" else "production"
_DEFAULT_DAGS_IMAGE_TAG = "dev" if ENV_SHORT_NAME == "dev" else "v1"
_DEFAULT_RUNTIME_IMAGE_TAG = "dev" if ENV_SHORT_NAME == "dev" else "v1"

_AIRFLOW_USER_UUID = 50000
# Pod-level: fs_group, run_as_non_root, run_as_user/group
DEFAULT_POD_SECURITY_CONTEXT = V1PodSecurityContext(
    run_as_non_root=True,
    run_as_user=_AIRFLOW_USER_UUID,
    run_as_group=_AIRFLOW_USER_UUID,
    fs_group=_AIRFLOW_USER_UUID,  # makes emptyDir volumes group-writable
)
# Container-level: for init containers using public images (alpine/git)
DEFAULT_CONTAINER_SECURITY_CONTEXT = V1SecurityContext(
    run_as_non_root=True,
)

_GIT_CLONE_SECURITY_CONTEXT = V1SecurityContext(
    run_as_non_root=False,
    run_as_user=0,
)

DEFAULT_CONTAINER_RESOURCES = V1ResourceRequirements(
    requests={"cpu": "0.2", "memory": "500Mi"},
    limits={"cpu": "0.5", "memory": "1Gi"},
)

default_env_vars = {
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
    "UV_CACHE_DIR": "/tmp/.cache/uv",
    "UV_LINK_MODE": "copy",
    "RUN_ID": "{{ run_id }}",
}
KPO_COMMON_DEFAULTS = dict(
    container_resources=DEFAULT_CONTAINER_RESOURCES,
    env_vars=default_env_vars,
)


def make_pod_name(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")[:253]


def get_registry_image(image_name: str) -> str:
    return f"{_REGISTRY}/{_REGISTRY_FOLDER}/{image_name}"


def _make_git_clone_command(
    repo_url: str,
    repo_name: str,
    branch: str,
    dest: str,
    sparse_paths: str | list[str] | None = None,
) -> str:
    """Build a shell command that clones a git repository and copies specified paths to a destination.

    When sparse_paths is provided, uses --filter=blob:none and --sparse to only
    download blobs for the specified paths, minimizing network transfer and memory footprint.
    When sparse_paths is None, performs a full shallow clone (--depth 1) of the repository.

    Only directories are supported as sparse_paths — individual files are not handled.

    Copying behaviour depends on the number of sparse_paths:
        - Single directory: contents are flattened into dest using cp -r <path>/. <dest>/.
          The directory itself is not created under dest — only its children are.
          Use this when the consumer expects files at the root of dest.
          Example: sparse_paths="orchestration/dags" → dest/common/, dest/jobs/
          Example: sparse_paths="jobs/etl/instagram" → dest/pyproject.toml, dest/src/
        - Multiple directories: each path is copied to dest/{path}, preserving full
          repository hierarchy so sibling paths do not collide.
          Example: sparse_paths=["common", "jobs"] → dest/common/, dest/jobs/

    Args:
        repo_url: URL of the git repository to clone.
        repo_name: Local name for the cloned repo under /tmp/.
        branch: Branch to clone.
        dest: Destination directory where paths are copied into.
        sparse_paths: One or more repo-relative directory paths to sparse-checkout and copy.
            - Single string or single-element list: contents are flattened into dest.
            - Multi-element list: each path is copied to dest/{path}, preserving hierarchy.
            - None: full repository contents are copied into dest.

    Returns:
        A shell command string suitable for use as a container entrypoint arg.

    """
    if sparse_paths is not None:
        if isinstance(sparse_paths, str):
            sparse_paths = [sparse_paths]

        anchored = " ".join(f"/{p}" for p in sparse_paths)

        if len(sparse_paths) == 1:
            copies = f"cp -r /tmp/{repo_name}/{sparse_paths[0]}/. {dest}/"
        else:
            copies = " && ".join(
                [
                    (
                        f"mkdir -p {dest}/{os.path.dirname(path)}"
                        f" && cp -r /tmp/{repo_name}/{path} {dest}/{path}"
                    )
                    for path in sparse_paths
                ]
            )

        return (
            f"echo '=== sparse-cloning {' '.join(sparse_paths)} from branch: {branch} ==='"
            f" && git clone --depth 1 --branch {branch} --filter=blob:none --sparse {repo_url} /tmp/{repo_name}"
            f" && cd /tmp/{repo_name}"
            f" && git sparse-checkout set --no-cone {anchored}"
            f" && set -x"
            f" && {copies}"
            f" && set +x"
            f" && echo '=== cleaning ==='"
            f" && rm -rf /tmp/{repo_name}"
            f" && echo '=== contents ==='"
            f" && ls {dest}/"
        )

    return (
        f"echo '=== cloning branch: {branch} ==='"
        f" && git clone --depth 1 --branch {branch} {repo_url} /tmp/{repo_name}"
        f" && cp -r /tmp/{repo_name}/. {dest}/"
        f" && echo '=== cleaning ==='"
        f" && rm -rf /tmp/{repo_name}"
        f" && echo '=== contents ==='"
        f" && ls {dest}/"
    )


def _make_orchestration_worker_pod_spec(dags_branch: str, dags_image_tag: str) -> V1Pod:
    """Pod spec for the non-celery orchestration worker that runs the git-sync init container in orchestration_mode='kubernetes'."""
    return V1Pod(
        spec=V1PodSpec(
            init_containers=[
                V1Container(
                    name="git-clone",
                    image="alpine/git",
                    command=["sh", "-c"],
                    args=[
                        _make_git_clone_command(
                            repo_url=_DAGS_REPO_URL,
                            repo_name=_DAGS_REPO_NAME,
                            branch=dags_branch,
                            sparse_paths=_DAGS_SPARSE_PATHS,
                            dest=_DAGS_MOUNT,
                        )
                    ],
                    volume_mounts=[
                        V1VolumeMount(name=_DAGS_VOLUME, mount_path=_DAGS_MOUNT)
                    ],
                    security_context=_GIT_CLONE_SECURITY_CONTEXT,
                )
            ],
            containers=[
                V1Container(
                    name="base",
                    image=f"{get_registry_image(_CELERY_WORKER_IMAGE_NAME)}:{dags_image_tag}",
                    volume_mounts=[
                        V1VolumeMount(name=_DAGS_VOLUME, mount_path=_DAGS_MOUNT)
                    ],
                )
            ],
        )
    )


def _make_job_worker_pod_spec(
    branch: str,
    sparse_paths: str | list[str],
    run_as_non_root: bool = True,
) -> V1Pod:
    """Pod spec for the job worker that runs the git-sync init container in runtime_mode='gitsynced'."""
    return V1Pod(
        spec=V1PodSpec(
            init_containers=[
                V1Container(
                    name="git-clone",
                    image="alpine/git",
                    command=["sh", "-c"],
                    args=[
                        _make_git_clone_command(
                            repo_url=_MS_REPO_URL,
                            repo_name=_MS_REPO_NAME,
                            branch=branch,
                            sparse_paths=sparse_paths,
                            dest=_MS_MOUNT,
                        )
                    ],
                    volume_mounts=[
                        V1VolumeMount(name=_MS_VOLUME, mount_path=_MS_MOUNT)
                    ],
                    security_context=_GIT_CLONE_SECURITY_CONTEXT,
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


class CustomKubernetesPodOperator(KubernetesPodOperator):
    """
    Opinionated wrapper around KubernetesPodOperator with two configuration axes:

    orchestration_mode:
      - "celery" (default): task runs on the default celery worker queue; worker pod is
        untouched (fully defined by the Airflow helm chart).
      - "kubernetes": task is routed to the kubernetes queue; the worker pod gets a
        git-sync init container that pulls the DAGs repo at `dag_branch`.
        `dag_branch` and `dag_image_tag` must be **literal strings** — they are embedded
        into executor_config at DAG parse time so the Kubernetes executor can inject
        the init container when it creates the worker pod. Jinja templates, XCom refs,
        and dynamic task mapping cannot be used here: executor_config is consumed by the
        scheduler before any template rendering occurs. To target a different branch or
        image tag, change the value in the DAG file and let the scheduler re-parse.

    runtime_mode:
      - "gitsynced": job pod clones the microservice repo at `branch` and runs the
        entrypoint via `uv run`. `branch` is a template field and is rendered before
        execute(), so Jinja expressions like "{{ params.branch }}" work here.
        when using this mode, the user must provide `microservice_path`, the runtime image and tags (optional defaults to the base Python image), and the command is always `uv run` with the provided arguments.
        Optionally, `runtime_sparse_paths` can be used to clone additional repo paths
        in the same job pod (for shared modules). When provided, `microservice_path`
        is still required and used as default runtime working directory.
      - "containerized": job pod uses a fully-built image; no init container. The user
        must provide `runtime_image=`.

    private_registry (containerized mode only, default True):
      - True: `runtime_image` is treated as a short name within the internal Artifact
        Registry. The full image ref is built as
        `{registry}/{registry_folder}/{runtime_image}:{runtime_image_tag}`.
        If `runtime_image` already starts with the full registry URL it is used
        as-is and only the tag is appended.
      - False: `runtime_image` is treated as a public / external image name (e.g.
        `"python"`, `"alpine"`). The full image ref is built as
        `{runtime_image}:{runtime_image_tag}` — no registry prefix is added.

    Parameters handled implicitly (must not appear in DAG code):
      git-sync init containers, volume wiring, executor_config / queue assignment,
      base image selection, uv entrypoint construction.
    """

    template_fields = tuple(KubernetesPodOperator.template_fields) + (
        "runtime_branch",
        "dag_branch",
        "runtime_image",
        "runtime_image_tag",
    )

    def __init__(
        self,
        *,
        runtime_mode: Literal["gitsynced", "containerized"],
        orchestration_mode: Literal["celery", "kubernetes"] = "celery",
        dag_branch: str = _DEFAULT_DAGS_BRANCH,
        dag_image_tag: str = _DEFAULT_DAGS_IMAGE_TAG,
        runtime_branch: str | None = None,
        microservice_path: str | None = None,
        runtime_sparse_paths: str | list[str] | None = None,
        runtime_workdir: str | None = None,
        runtime_image: str | None = None,
        runtime_image_tag: str = _DEFAULT_RUNTIME_IMAGE_TAG,
        private_registry: bool = True,
        namespace: str = AIRFLOW_NAMESPACE,
        service_account_name: str = "airflow-worker",
        in_cluster: bool | None = None,
        get_logs: bool = True,
        is_delete_operator_pod: bool = True,
        on_finish_action: str = "delete_pod",
        image_pull_policy: str | None = None,
        kubernetes_conn_id: str | None = None,
        **kwargs,
    ):
        self.runtime_mode = runtime_mode
        self.orchestration_mode = orchestration_mode
        self.dag_branch = dag_branch
        self.dag_image_tag = dag_image_tag
        self.runtime_branch = runtime_branch
        self.microservice_path = microservice_path
        self.runtime_sparse_paths: list[str] | None = None
        self.runtime_workdir: str = ""

        if runtime_mode == "gitsynced":
            if runtime_branch is None:
                raise ValueError(
                    "runtime_branch is required for runtime_mode='gitsynced'"
                )
            if microservice_path is None:
                raise ValueError(
                    "microservice_path is required for runtime_mode='gitsynced'"
                )

            if runtime_sparse_paths is None:
                self.runtime_sparse_paths = [microservice_path]
            else:
                self.runtime_sparse_paths = (
                    runtime_sparse_paths
                    if isinstance(runtime_sparse_paths, list)
                    else [runtime_sparse_paths]
                )

            self.runtime_workdir = runtime_workdir or microservice_path
        self.runtime_image = runtime_image
        self.runtime_image_tag = runtime_image_tag
        self.private_registry = private_registry

        if private_registry:
            kwargs["security_context"] = DEFAULT_POD_SECURITY_CONTEXT

        kwargs.setdefault(
            "name",
            make_pod_name(kwargs.get("name", kwargs.get("task_id", ""))),
        )

        if in_cluster is None:
            in_cluster = not bool(LOCAL_ENV)
        if image_pull_policy is None:
            image_pull_policy = (
                "Always"  # if ENV_SHORT_NAME == "dev" else "IfNotPresent"
            )
        if kubernetes_conn_id is None:
            kubernetes_conn_id = "kubernetes_default" if LOCAL_ENV else None

        extra_env = kwargs.get("env_vars")
        if extra_env is None:
            kwargs["env_vars"] = default_env_vars
        elif isinstance(extra_env, dict):
            kwargs["env_vars"] = {**default_env_vars, **extra_env}

        if runtime_mode == "gitsynced":
            if runtime_image is not None:
                kwargs["image"] = (
                    f"{runtime_image if runtime_image.startswith(_REGISTRY) else get_registry_image(runtime_image)}:{runtime_image_tag}"
                )
            else:
                kwargs.setdefault(
                    "image",
                    f"{get_registry_image(_BASE_PYTHON_IMAGE_NAME)}:{runtime_image_tag}",
                )

            if "cmds" not in kwargs:
                kwargs["cmds"] = ["sh", "-c"]

            if "arguments" in kwargs:
                # Join all items into a single shell string so the split-list format
                # (["script.py", "--flag", "value"]) works identically to containerized.
                # Limitation: argument values that contain spaces will be word-split by
                # the shell — avoid spaces in values or wrap them in shell quotes.
                kwargs["arguments"] = [
                    f"cd {_MS_MOUNT}/{self.runtime_workdir} && uv run --no-cache {' '.join(str(a) for a in kwargs['arguments'])}"
                ]

            kwargs["env_vars"]["UV_CACHE_DIR"] = f"{_MS_MOUNT}/.cache/uv"

        elif runtime_mode == "containerized":
            if runtime_image is None:
                raise ValueError(
                    "runtime_image is required for runtime_mode='containerized'"
                )
            if not private_registry:
                kwargs["image"] = f"{runtime_image}:{runtime_image_tag}"
            elif runtime_image.startswith(_REGISTRY):
                kwargs["image"] = f"{runtime_image}:{runtime_image_tag}"
            else:
                kwargs["image"] = (
                    f"{get_registry_image(runtime_image)}:{runtime_image_tag}"
                )

        if orchestration_mode == "kubernetes":
            kwargs["queue"] = "kubernetes"
            # Must be set at __init__ time: the scheduler reads executor_config when
            # it creates the worker pod, before the pod exists and before any Jinja
            # rendering. dag_branch and dag_image_tag must therefore be literal strings.
            kwargs["executor_config"] = {
                "pod_override": _make_orchestration_worker_pod_spec(
                    dag_branch, dag_image_tag
                )
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
            self.full_pod_spec = _make_job_worker_pod_spec(
                self.runtime_branch,
                self.runtime_sparse_paths
                if self.runtime_sparse_paths
                else self.microservice_path,
                run_as_non_root=self.private_registry,
            )
        return super().execute(context)
