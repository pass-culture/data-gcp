import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from airflow.models.dag import DagContext
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from common.config import ENVIRONMENT_NAME, GCS_AIRFLOW_BUCKET
from kubernetes.client import (
    V1PersistentVolumeClaimVolumeSource,
    V1Volume,
    V1VolumeMount,
)

logger = logging.getLogger(__name__)


DEFAULT_PVC_NAME = f"airflow-runtime-storage-{ENVIRONMENT_NAME}"
DEFAULT_MOUNT_PATH = "/shared"
DEFAULT_GCS_BASE_PREFIX = "shared"
GCS_FUSE_ANNOTATIONS = {"gke-gcsfuse/volumes": "true"}


def _make_gcs_folder_task(bucket_name: str, folder: str) -> Callable:
    def _run():
        import tempfile

        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook()
        with tempfile.NamedTemporaryFile() as tmp:
            hook.upload(
                bucket_name=bucket_name,
                object_name=f"{folder}/",
                filename=tmp.name,
                mime_type="application/x-www-form-urlencoded",
            )
        logger.info("Created gs://%s/%s/", bucket_name, folder)

    return PythonOperator(task_id="create_gcs_folder", python_callable=_run)


@dataclass
class SharedPVCVolume:
    """GCS FUSE-backed PVC volume + mount pair.

    PV and PVC are assumed permanent (provisioned via Helm/Terraform).
    sub_path scopes the mount to a folder within the bucket — set to
    f"{dag_id}/{run_id}" for per-run isolation.

    Note: sub_path is resolved at DAG parse time and passed to
    Kubernetes — Jinja templates are NOT rendered inside V1VolumeMount.
    Use dag_id alone for mount-level scoping (with max_active_runs=1) and
    pass RUN_ID as an env var for file-level isolation within the mount.
    """

    pvc_name: str
    mount_path: str
    sub_path: str
    name: str = field(default="shared-data")

    @property
    def volume(self) -> V1Volume:
        return V1Volume(
            name=self.name,
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                claim_name=self.pvc_name
            ),
        )

    @property
    def mount(self) -> V1VolumeMount:
        return V1VolumeMount(
            name=self.name,
            mount_path=self.mount_path,
            sub_path=self.sub_path,
        )

    @property
    def annotations(self) -> dict[str, str]:
        return GCS_FUSE_ANNOTATIONS

    def kpo_kwargs(
        self, extra_env_vars: dict[str, str] | None = None
    ) -> dict[str, Any]:
        env_vars = {"RUN_ID": "{{ run_id }}", **(extra_env_vars or {})}
        return {
            "volumes": [self.volume],
            "volume_mounts": [self.mount],
            "annotations": self.annotations,
            "env_vars": env_vars,
        }


@dataclass
class StorageLifecycle:
    """Lifecycle manager for a GCS FUSE-backed shared volume.

    Attributes:
        setup: TaskGroup that creates the scoped GCS folder.
        teardown: TaskGroup that deletes the scoped GCS folder.
        gcs_folder: Resolved GCS folder path (without ``gs://`` prefix).
        gcs_run_folder: Resolved GCS folder path for the current run (without ``gs://`` prefix).
        shared_volume: :class:`SharedPVCVolume` instance ready to inject into
            KubernetesPodOperator via ``**storage.shared_volume.kpo_kwargs``.
    """

    setup: TaskGroup
    teardown: TaskGroup
    gcs_folder: str
    gcs_run_folder: str
    shared_volume: SharedPVCVolume


def make_storage_lifecycle(
    bucket_name: str = GCS_AIRFLOW_BUCKET,
    pvc_name: str = DEFAULT_PVC_NAME,
    mount_path: str = DEFAULT_MOUNT_PATH,
    gcs_base_prefix: str = DEFAULT_GCS_BASE_PREFIX,
    sub_path: str | None = None,
) -> StorageLifecycle:
    """Create GCS folder setup/teardown tasks for a GCS FUSE-backed shared volume.

    The PV and PVC are assumed permanent (provisioned via Helm/Terraform).
    This function only manages the GCS folder lifecycle: creates the folder
    on setup and deletes it on teardown.

    When ``sub_path`` is not provided, ``dag_id`` is resolved automatically
    from the active DAG context and used to scope the GCS folder and volume
    mount to ``"{gcs_base_prefix}/{dag_id}"``. Must be called inside an active
    DAG context in that case.

    For file-level run isolation within the mount, pass ``RUN_ID="{{ run_id }}"``
    in your operator's ``env_vars`` — Jinja is rendered in env vars at execution
    time, unlike ``V1VolumeMount.sub_path`` which is parse-time only.

    Args:
        bucket_name: GCS bucket backing the FUSE-mounted PVC.
            Defaults to ``GCS_AIRFLOW_BUCKET``.
        pvc_name: Name of the pre-existing PersistentVolumeClaim.
            Defaults to ``"airflow-runtime-storage-{ENV_SHORT_NAME}"``.
        mount_path: Filesystem path where the volume is mounted inside pods.
            Defaults to ``"/shared"``.
        gcs_base_prefix: Top-level GCS prefix under which the scoped folder
            is created. Defaults to ``"shared"``.
        sub_path: Kubernetes sub_path for the volume mount, scoping the mount
            to a subdirectory within the bucket. When not provided, resolved
            from the active DAG context as ``"{gcs_base_prefix}/{dag_id}"``.
            Pass an explicit value for custom isolation — DAG context is not
            required in that case.

    Returns:
        StorageLifecycle: Dataclass with setup/teardown TaskGroups, resolved
            ``gcs_folder``, and a ``shared_volume`` ready to inject into
            KubernetesPodOperator via ``**storage.shared_volume.kpo_kwargs``.

    Raises:
        RuntimeError: If ``sub_path`` is not provided and the function is called
            outside of an active DAG context.
    """
    if sub_path is not None:
        resolved_sub_path = sub_path
        gcs_folder = f"{gcs_base_prefix}/{sub_path}"
    else:
        dag = DagContext.get_current_dag()
        if dag is None:
            msg = (
                "make_storage_lifecycle must be called inside a `with DAG(...)` block "
                "when sub_path is not provided."
            )
            logger.error(msg)
            raise RuntimeError(msg)

        resolved_sub_path = f"{gcs_base_prefix}/{dag.dag_id}"
        gcs_folder = f"{gcs_base_prefix}/{dag.dag_id}"

    shared = SharedPVCVolume(
        pvc_name=pvc_name,
        mount_path=mount_path,
        sub_path=resolved_sub_path,
    )

    with TaskGroup("storage_setup") as setup:
        _make_gcs_folder_task(bucket_name=bucket_name, folder=gcs_folder)

    with TaskGroup("storage_teardown") as teardown:
        GCSDeleteObjectsOperator(
            task_id="delete_gcs_folder",
            bucket_name=bucket_name,
            prefix=f"{gcs_folder}/",
        )

    return StorageLifecycle(
        setup=setup,
        teardown=teardown,
        gcs_folder=gcs_folder,
        gcs_run_folder=f"{gcs_folder}/{{{{ run_id }}}}",
        shared_volume=shared,
    )
