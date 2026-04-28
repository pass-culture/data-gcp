import base64
import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from kubernetes.client import (
    V1Container,
    V1EnvVar,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
)

# --- Config ---
REPO = "https://github.com/pass-culture/data-gcp"
DAG_PATH_IN_REPO = "orchestration/dags"
GIT_SYNC_ROOT = "/git"
GIT_SYNC_DEST = "repo"


NAMESPACE = "airflow-development"
GCS_BUCKET = "airflow-data-bucket-dev"
GCS_FOLDER = "tmp_folder"
# PV_NAME = "demo-pv"
PVC_NAME = "demo-pvc"

# --- Scripts ---
WRITE_SCRIPT = """
import os, json, datetime

data = {
    "written_by": "write_task",
    "timestamp": datetime.datetime.utcnow().isoformat(),
    "message": "hello from the write pod",
}
os.makedirs("/shared", exist_ok=True)
with open("/shared/output.json", "w") as f:
    json.dump(data, f, indent=2)
print("Written to /shared/output.json:")
print(json.dumps(data, indent=2))
"""

READ_SCRIPT = """
import json

with open("/shared/output.json", "r") as f:
    data = json.load(f)
print("Read from /shared/output.json:")
print(json.dumps(data, indent=2))
print("Read task done.")
"""


def encode_script(script: str) -> str:
    return base64.b64encode(script.encode()).decode()


def git_sync_override(branch: str) -> dict:
    return {
        "pod_override": V1Pod(
            spec=V1PodSpec(
                init_containers=[
                    V1Container(
                        name="git-sync",
                        image="registry.k8s.io/git-sync/git-sync:v4.2.1",
                        env=[
                            V1EnvVar(name="GITSYNC_REPO", value=REPO),
                            V1EnvVar(name="GITSYNC_BRANCH", value=branch),
                            V1EnvVar(name="GITSYNC_DEPTH", value="1"),
                            V1EnvVar(name="GITSYNC_ROOT", value=GIT_SYNC_ROOT),
                            V1EnvVar(name="GITSYNC_DEST", value=GIT_SYNC_DEST),
                            V1EnvVar(name="GITSYNC_ONE_TIME", value="true"),
                        ],
                        volume_mounts=[
                            V1VolumeMount(name="airflow-dags", mount_path=GIT_SYNC_ROOT)
                        ],
                    )
                ],
                containers=[
                    V1Container(
                        name="base",
                        volume_mounts=[
                            V1VolumeMount(
                                name="airflow-dags",
                                mount_path="/opt/airflow/dags",
                                sub_path=f"{GIT_SYNC_DEST}/{DAG_PATH_IN_REPO}",
                            )
                        ],
                    )
                ],
            )
        )
    }


# --- Kubernetes manifests ---

PVC_YAML = f"""
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {PVC_NAME}
  namespace: {NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: standard-rwo
"""

# --- Shared K8s volume config ---

shared_volume = V1Volume(
    name="shared-data",
    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
)

shared_volume_mount = V1VolumeMount(
    name="shared-data",
    mount_path="/shared",
)

resources = V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "256Mi"},
    limits={"cpu": "200m", "memory": "512Mi"},
)

# --- Shared KPO kwargs ---
kpo_common = dict(
    namespace=NAMESPACE,
    image="python:3.12-slim",
    cmds=["sh", "-c"],
    volumes=[shared_volume],
    volume_mounts=[shared_volume_mount],
    container_resources=resources,
    env_vars={"GCS_BUCKET": GCS_BUCKET, "GCS_FOLDER": GCS_FOLDER},
    in_cluster=True,
    get_logs=True,
    is_delete_operator_pod=True,
    on_finish_action="delete_pod",
    image_pull_policy="IfNotPresent",
    service_account_name="airflow-worker",
)

# --- DAG ---

default_args = {
    "start_date": datetime.datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="demo_pvc_gcs_pipeline",
    default_args=default_args,
    description="Demo: GCS folder + PV/PVC lifecycle with write/read KPO tasks",
    schedule_interval=None,
    catchup=False,
    tags=["demo", "pvc", "gcs"],
    params={
        "branch": Param(
            default="k8s-social-network",
            type="string",
        ),
    },
):

    @task(task_id="create_gcs_folder")
    def create_gcs_folder():
        import tempfile

        hook = GCSHook()
        with tempfile.NamedTemporaryFile() as tmp:
            hook.upload(
                bucket_name=GCS_BUCKET,
                object_name=f"{GCS_FOLDER}/",
                filename=tmp.name,
                mime_type="application/x-www-form-urlencoded",
            )
        print(f"Created gs://{GCS_BUCKET}/{GCS_FOLDER}/")

    # 2. Create PVC — will bind to the PV above
    create_pvc = KubernetesCreateResourceOperator(
        task_id="create_pvc",
        yaml_conf=PVC_YAML,
        kubernetes_conn_id="kubernetes_default",
    )

    # 3. Write task
    write_task = KubernetesPodOperator(
        task_id="write_task",
        name="demo-write-task",
        arguments=[
            f"pip install google-cloud-storage -q --root-user-action=ignore "
            f"&& echo '{encode_script(WRITE_SCRIPT)}' | base64 -d | python"
        ],
        **kpo_common,
        pod_override=git_sync_override(branch="{{ params.branch }}")["pod_override"],
        queue="kubernetes",
    )

    # 4. Read task
    read_task = KubernetesPodOperator(
        task_id="read_task",
        name="demo-read-task",
        arguments=[
            f"pip install google-cloud-storage -q --root-user-action=ignore "
            f"&& echo '{encode_script(READ_SCRIPT)}' | base64 -d | python"
        ],
        **kpo_common,
        pod_override=git_sync_override(branch="{{ params.branch }}")["pod_override"],
        queue="kubernetes",
    )

    # 5a. PVC Cleanup
    delete_pvc = KubernetesDeleteResourceOperator(
        task_id="delete_pvc",
        yaml_conf=PVC_YAML,
        kubernetes_conn_id="kubernetes_default",
    )

    # 5b. GCS cleanup — independent of PVC teardown
    delete_gcs_folder = GCSDeleteObjectsOperator(
        task_id="delete_gcs_folder",
        bucket_name=GCS_BUCKET,
        prefix=f"{GCS_FOLDER}/",
    )

    # --- Pipeline ---
    gcs_setup = create_gcs_folder()

    gcs_setup >> create_pvc >> write_task >> read_task

    # Cleanup
    read_task >> [delete_pvc, delete_gcs_folder]
