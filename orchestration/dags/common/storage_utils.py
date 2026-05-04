from dataclasses import dataclass, field
from typing import Callable

from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from kubernetes.client import (
    V1PersistentVolumeClaimVolumeSource,
    V1Volume,
    V1VolumeMount,
)


def _make_gcs_dag_folder_task(
    bucket_name: str,
    dag_id: str,
    base_prefix: str = "tmp",
) -> tuple[Callable, str]:
    """Return (create_operator_fn, folder) for a DAG-scoped GCS scratch folder.

    folder = f"{base_prefix}/{dag_id}" — unique per DAG, stable across runs.
    Call create_operator_fn() inside the DAG/TaskGroup context to register the task.
    Pass folder to GCSDeleteObjectsOperator(prefix=f"{folder}/") for teardown.
    """
    folder = f"{base_prefix}/{dag_id}"

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
        print(f"Created gs://{bucket_name}/{folder}/")

    def _create_operator():
        return PythonOperator(
            task_id="create_gcs_folder",
            python_callable=_run,
        )

    return _create_operator, folder


def _make_pvc_yaml(
    name: str,
    namespace: str,
    storage: str = "1Gi",
    storage_class: str = "standard-rwo",
    access_mode: str = "ReadWriteOnce",
) -> str:
    return f"""apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {name}
  namespace: {namespace}
spec:
  accessModes:
    - {access_mode}
  resources:
    requests:
      storage: {storage}
  storageClassName: {storage_class}
"""


@dataclass
class SharedPVCVolume:
    """Pairs a PVC-backed V1Volume with its V1VolumeMount.

    Use `.kpo_kwargs` to inject both into any KubernetesPodOperator call:
        EasyKubernetesPodOperator(..., **shared.kpo_kwargs)
    """

    pvc_name: str
    mount_path: str
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
        return V1VolumeMount(name=self.name, mount_path=self.mount_path)

    @property
    def kpo_kwargs(self) -> dict:
        return {"volumes": [self.volume], "volume_mounts": [self.mount]}


@dataclass
class StorageLifecycle:
    setup: TaskGroup
    teardown: TaskGroup
    gcs_folder: str
    shared_volume: SharedPVCVolume


def make_storage_lifecycle(
    bucket_name: str,
    dag_id: str,
    pvc_name: str,
    namespace: str,
    pvc_storage: str = "1Gi",
    pvc_storage_class: str = "standard-rwo",
    gcs_base_prefix: str = "tmp",
    mount_path: str = "/shared",
    kubernetes_conn_id: str = "kubernetes_default",
) -> StorageLifecycle:
    """Setup/teardown TaskGroups for a GCS scratch folder + Kubernetes PVC.

    Must be called inside an active DAG context (inside `with DAG(...)`).

    Returns a StorageLifecycle:
      .setup          — TaskGroup: create_gcs_folder + create_pvc run in parallel
      .teardown       — TaskGroup: delete_pvc + delete_gcs_folder run in parallel
      .gcs_folder     — computed path "{gcs_base_prefix}/{dag_id}"
      .shared_volume  — SharedPVCVolume; spread into KPO with **storage.shared_volume.kpo_kwargs

    Usage:
        storage = make_storage_lifecycle(bucket_name=..., dag_id=..., ...)
        storage.setup >> your_tasks >> storage.teardown
    """
    pvc_yaml = _make_pvc_yaml(
        name=pvc_name,
        namespace=namespace,
        storage=pvc_storage,
        storage_class=pvc_storage_class,
    )
    create_gcs_folder, gcs_folder = _make_gcs_dag_folder_task(
        bucket_name=bucket_name,
        dag_id=dag_id,
        base_prefix=gcs_base_prefix,
    )
    shared = SharedPVCVolume(pvc_name=pvc_name, mount_path=mount_path)

    with TaskGroup("storage_setup") as setup:
        # No >> between these: GCS and PVC creation are independent, run in parallel.
        # Both must finish before the group's downstream tasks start (TaskGroup fan-in).
        create_gcs_folder()
        KubernetesCreateResourceOperator(
            task_id="create_pvc",
            yaml_conf=pvc_yaml,
            kubernetes_conn_id=kubernetes_conn_id,
        )

    with TaskGroup("storage_teardown") as teardown:
        # Same: PVC and GCS deletion are independent, run in parallel.
        KubernetesDeleteResourceOperator(
            task_id="delete_pvc",
            yaml_conf=pvc_yaml,
            kubernetes_conn_id=kubernetes_conn_id,
        )
        GCSDeleteObjectsOperator(
            task_id="delete_gcs_folder",
            bucket_name=bucket_name,
            prefix=f"{gcs_folder}/",
        )

    return StorageLifecycle(
        setup=setup,
        teardown=teardown,
        gcs_folder=gcs_folder,
        shared_volume=shared,
    )
