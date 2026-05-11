import base64
import logging
from dataclasses import dataclass, field
from typing import Any

from airflow.models.dag import DagContext
from airflow.utils.task_group import TaskGroup
from common.config import AIRFLOW_NAMESPACE, ENV_SHORT_NAME
from common.operators.kubernetes import (
    _DEFAULT_DAGS_IMAGE_TAG,
    EasyKubernetesPodOperator,
)
from kubernetes.client import (
    V1PersistentVolumeClaimVolumeSource,
    V1Volume,
    V1VolumeMount,
)

logger = logging.getLogger(__name__)

DEFAULT_STORAGE_CLASS = "standard-rwo"
DEFAULT_STORAGE_SIZE = "10Gi"
DEFAULT_MOUNT_PATH = "/shared"


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _encode_script(script: str) -> str:
    """Base64-encode a Python script for inline shell execution."""
    return base64.b64encode(script.encode()).decode()


def _make_create_pvc_task(
    pvc_name: str,
    namespace: str,
    storage_class: str,
    storage_size: str,
) -> EasyKubernetesPodOperator:
    """Return a task that creates a namespaced PVC via the in-cluster Kubernetes API.

    The script is base64-encoded and piped to Python at runtime to avoid shell
    escaping issues. Uses the airflow image with the scheduler service account.

    Args:
        pvc_name: Name of the PVC to create.
        namespace: Kubernetes namespace where the PVC is created.
        storage_class: StorageClass to use (e.g. ``"standard-rwo"``).
        storage_size: Requested storage size (e.g. ``"10Gi"``).
    """
    script = f"""
from kubernetes import client, config
config.load_incluster_config()
v1 = client.CoreV1Api()
body = client.V1PersistentVolumeClaim(
    metadata=client.V1ObjectMeta(name="{pvc_name}"),
    spec=client.V1PersistentVolumeClaimSpec(
        access_modes=["ReadWriteOnce"],
        resources=client.V1ResourceRequirements(
            requests={{"storage": "{storage_size}"}}
        ),
        storage_class_name="{storage_class}",
    ),
)
v1.create_namespaced_persistent_volume_claim(namespace="{namespace}", body=body)
print("Created PVC {namespace}/{pvc_name} ({storage_class}, {storage_size})")
"""
    return EasyKubernetesPodOperator(
        task_id="create_pvc",
        orchestration_mode="kubernetes",
        runtime_mode="containerized",
        runtime_image="airflow",
        runtime_image_tag=_DEFAULT_DAGS_IMAGE_TAG,
        private_registry=True,
        service_account_name="airflow-scheduler",
        cmds=["sh", "-c"],
        arguments=[f"echo '{_encode_script(script)}' | base64 -d | python"],
    )


def _make_delete_pvc_task(
    pvc_name: str,
    namespace: str,
) -> EasyKubernetesPodOperator:
    """Return a task that deletes a namespaced PVC via the in-cluster Kubernetes API.

    Deleting the PVC also triggers deletion of the underlying GCE Persistent
    Disk (reclaimPolicy: Delete on the ``standard-rwo`` StorageClass).

    Args:
        pvc_name: Name of the PVC to delete.
        namespace: Kubernetes namespace where the PVC lives.
    """
    script = f"""
from kubernetes import client, config
config.load_incluster_config()
client.CoreV1Api().delete_namespaced_persistent_volume_claim(
    name="{pvc_name}", namespace="{namespace}"
)
print("Deleted PVC {namespace}/{pvc_name}")
"""
    return EasyKubernetesPodOperator(
        task_id="delete_pvc",
        orchestration_mode="kubernetes",
        runtime_mode="containerized",
        runtime_image="airflow",
        runtime_image_tag=_DEFAULT_DAGS_IMAGE_TAG,
        private_registry=True,
        service_account_name="airflow-scheduler",
        cmds=["sh", "-c"],
        arguments=[f"echo '{_encode_script(script)}' | base64 -d | python"],
    )


# ─── Public API ───────────────────────────────────────────────────────────────


@dataclass
class SharedVolume:
    """Dynamically provisioned PVC volume + mount pair.

    The PVC is created at DAG run time by `create_pvc` and deleted by
    `delete_pvc`. The PV is provisioned automatically by the `standard-rwo`
    StorageClass provisioner (pd.csi.storage.gke.io) when the PVC is created
    and deleted automatically when the PVC is deleted (reclaimPolicy: Delete).

    PVC naming convention: `airflow-{env}-pvc-{dag_id}`.
    """

    pvc_name: str
    mount_path: str
    _name: str = field(default="shared-storage", repr=False)

    @property
    def volume(self) -> V1Volume:
        return V1Volume(
            name=self._name,
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                claim_name=self.pvc_name,
            ),
        )

    @property
    def mount(self) -> V1VolumeMount:
        return V1VolumeMount(
            name=self._name,
            mount_path=self.mount_path,
        )

    def kpo_kwargs(self) -> dict[str, Any]:
        """Return kwargs to inject into EasyKubernetesPodOperator."""
        return {
            "volumes": [self.volume],
            "volume_mounts": [self.mount],
        }


@dataclass
class StorageLifecycle:
    """Lifecycle manager for a dynamically provisioned shared PVC.

    Attributes:
        setup: TaskGroup that creates the PVC at run time.
        teardown: TaskGroup that deletes the PVC (and underlying GCE disk)
            at run time.
        shared_volume: :class:`SharedVolume` instance. Use
            ``**storage.kpo_kwargs()`` to inject volumes and mounts into a
            KubernetesPodOperator in one call.

    Usage::

        storage = make_storage_lifecycle(storage_size="20Gi")

        task = EasyKubernetesPodOperator(
            task_id="my_task",
            ...
            **storage.kpo_kwargs(),
        )

        storage.setup >> task >> storage.teardown
    """

    setup: TaskGroup
    teardown: TaskGroup
    shared_volume: SharedVolume

    def kpo_kwargs(self) -> dict[str, Any]:
        """Proxy to shared_volume.kpo_kwargs() for convenience."""
        return self.shared_volume.kpo_kwargs()


def make_storage_lifecycle(
    storage_size: str = DEFAULT_STORAGE_SIZE,
    mount_path: str = DEFAULT_MOUNT_PATH,
    storage_class: str = DEFAULT_STORAGE_CLASS,
    namespace: str = AIRFLOW_NAMESPACE,
) -> StorageLifecycle:
    """Create a dynamic PVC setup/teardown lifecycle for cross-task data sharing.

    The PVC is created at run time by a `create_pvc` task and deleted by a
    `delete_pvc` task. The underlying GCE Persistent Disk is provisioned and
    destroyed automatically by the `standard-rwo` StorageClass.

    PVC naming follows the convention `airflow-{env}-pvc-{dag_id}`, enforced
    by a Kyverno ClusterPolicy (`restrict-airflow-pvc`) on the `data-ehp`
    cluster.

    **Design constraints**

    - ReadWriteOnce: only one node can mount the PVC at a time. Tasks sharing
      the volume must run sequentially.
    - max_active_runs=1 must be set on the DAG to prevent concurrent runs
      sharing the same PVC (name is fixed at parse time).
    - ReclaimPolicy Delete: deleting the PVC automatically deletes the GCE disk.

    **Usage**::

        with DAG(..., max_active_runs=1):
            storage = make_storage_lifecycle(storage_size="20Gi")

            task_a = EasyKubernetesPodOperator(
                task_id="task_a",
                ...
                **storage.kpo_kwargs(),
            )
            task_b = EasyKubernetesPodOperator(
                task_id="task_b",
                ...
                **storage.kpo_kwargs(),
            )

            storage.setup >> task_a >> task_b >> storage.teardown

    Args:
        storage_size: GCE Persistent Disk size. Defaults to ``"10Gi"``.
        mount_path: Filesystem path where the volume is mounted inside pods.
            Defaults to ``"/shared"``.
        storage_class: Kubernetes StorageClass. Defaults to ``"standard-rwo"``.
        namespace: Kubernetes namespace. Defaults to the Airflow namespace
            for the current environment.

    Returns:
        StorageLifecycle: Dataclass with ``setup``/``teardown`` TaskGroups
            and a ``shared_volume`` ready for volume wiring.

    Raises:
        RuntimeError: If called outside of an active ``with DAG(...)`` block.
    """
    dag = DagContext.get_current_dag()
    if dag is None:
        msg = "make_storage_lifecycle must be called inside a `with DAG(...)` block."
        logger.error(msg)
        raise RuntimeError(msg)

    pvc_name = f"airflow-{ENV_SHORT_NAME}-pvc-{dag.dag_id.replace('_', '-')}"

    shared = SharedVolume(pvc_name=pvc_name, mount_path=mount_path)

    with TaskGroup("storage_setup") as setup:
        _make_create_pvc_task(
            pvc_name=pvc_name,
            namespace=namespace,
            storage_class=storage_class,
            storage_size=storage_size,
        )

    with TaskGroup("storage_teardown") as teardown:
        _make_delete_pvc_task(
            pvc_name=pvc_name,
            namespace=namespace,
        )

    return StorageLifecycle(
        setup=setup,
        teardown=teardown,
        shared_volume=shared,
    )
