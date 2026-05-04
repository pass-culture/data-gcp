import base64
import datetime

from airflow import DAG
from common.config import GCS_AIRFLOW_BUCKET
from common.operators.kubernetes import EasyKubernetesPodOperator
from common.storage_utils_v2 import DEFAULT_PVC_NAME, make_storage_lifecycle
from kubernetes.client import V1ResourceRequirements

# --- Config ---
DAG_ID = "kpo_shared_volume_example"

# --- Scripts ---
WRITE_SCRIPT = """
import os, json, datetime

run_id = os.environ["RUN_ID"]
output_dir = f"/shared/{run_id}"
os.makedirs(output_dir, exist_ok=True)

data = {
    "written_by": "write_task",
    "timestamp": datetime.datetime.utcnow().isoformat(),
    "message": "hello from the write pod",
}
output_path = f"{output_dir}/output.json"
with open(output_path, "w") as f:
    json.dump(data, f, indent=2)
print(f"Written to {output_path}:")
print(json.dumps(data, indent=2))
"""

READ_SCRIPT = """
import os, json

run_id = os.environ["RUN_ID"]
output_path = f"/shared/{run_id}/output.json"
with open(output_path, "r") as f:
    data = json.load(f)
print(f"Read from {output_path}:")
print(json.dumps(data, indent=2))
print("Read task done.")
"""


def encode_script(script: str) -> str:
    return base64.b64encode(script.encode()).decode()


resources = V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "256Mi"},
    limits={"cpu": "200m", "memory": "512Mi"},
)

default_args = {
    "start_date": datetime.datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Demo: GCS FUSE shared volume with write/read KPO tasks",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["demo", "pvc"],
):
    storage = make_storage_lifecycle(
        bucket_name=GCS_AIRFLOW_BUCKET,
        pvc_name=DEFAULT_PVC_NAME,
    )

    write_task = EasyKubernetesPodOperator(
        task_id="write_task",
        runtime_mode="containerized",
        runtime_image="python:3.12-slim",
        volumes=[storage.shared_volume.volume],
        volume_mounts=[storage.shared_volume.mount],
        annotations=storage.shared_volume.annotations,
        arguments=[f"echo '{encode_script(WRITE_SCRIPT)}' | base64 -d | python"],
        env_vars={"GCS_FOLDER": storage.gcs_run_folder},
        container_resources=resources,
    )

    read_task = EasyKubernetesPodOperator(
        task_id="read_task",
        runtime_mode="containerized",
        runtime_image="python:3.12-slim",
        volumes=[storage.shared_volume.volume],
        volume_mounts=[storage.shared_volume.mount],
        annotations=storage.shared_volume.annotations,
        arguments=[f"echo '{encode_script(READ_SCRIPT)}' | base64 -d | python"],
        env_vars={"GCS_FOLDER": storage.gcs_run_folder},
        container_resources=resources,
    )

    storage.setup >> write_task >> read_task >> storage.teardown
