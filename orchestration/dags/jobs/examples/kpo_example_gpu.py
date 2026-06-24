import base64
import datetime

from airflow import DAG
from common.dynamic_pvc import make_storage_lifecycle
from common.operators.kubernetes import CustomKubernetesPodOperator
from kubernetes.client import V1ResourceRequirements, V1Toleration

container_resources = V1ResourceRequirements(
    requests={"cpu": "1", "memory": "1Gi"},
    limits={"nvidia.com/gpu": "1"},
)
# creer une toleration a la teinte
gpu_tolerations = [
    V1Toleration(
        key="gpu-n1-s-8",  # this is from terraform
        operator="Equal",
        value="true",  # same
        effect="NoSchedule",  # same
    ),
    V1Toleration(
        key="nvidia.com/gpu", operator="Equal", value="present", effect="NoSchedule"
    ),
]

# Node selector optionnel car on a qu'1 seul pool avec des gpu
gpu_node_selector = {
    "cloud.google.com/gke-nodepool": "data-gpu-node-pool-n1-standard-8"
}


DAG_ID = "kpo_example_gpu"
MOUNT_PATH = "/shared"


def encode_script(script: str) -> str:
    return base64.b64encode(script.encode()).decode()


WRITE_SCRIPT = f"""
import json, datetime
import time

output_path = "{MOUNT_PATH}/output.json"
data = {{
    "written_by": "write_task",
    "timestamp": datetime.datetime.utcnow().isoformat(),
    "message": "hello from the write pod",
}}
with open(output_path, "w") as f:
    json.dump(data, f, indent=2)
print(f"Written to {{output_path}}:")
print(json.dumps(data, indent=2))
print("sleeping 2 minutes")
time.sleep(120)
"""

READ_SCRIPT = f"""
import json

output_path = "{MOUNT_PATH}/output.json"
with open(output_path, "r") as f:
    data = json.load(f)
print(f"Read from {{output_path}}:")
print(json.dumps(data, indent=2))
print("Read task done.")

"""


default_args = {
    "start_date": datetime.datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Demo: GPU request",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["example", "kubernetes"],
):
    storage = make_storage_lifecycle(storage_size="10Gi")

    # Optional: we extract the volume and mount info from the storage lifecycle to demonstrate how it can be used directly.
    # In practice, you would likely just use **storage.kpo_kwargs() in your tasks and not worry about the explicit volume/mount objects.
    volume = storage.kpo_kwargs()["volumes"]
    mount = storage.kpo_kwargs()["volume_mounts"]

    write_task = CustomKubernetesPodOperator(
        task_id="write_task",
        runtime_mode="containerized",
        private_registry=False,
        runtime_image="python",
        runtime_image_tag="3.12-slim",
        cmds=["sh", "-c"],
        arguments=[f"echo '{encode_script(WRITE_SCRIPT)}' | base64 -d | python"],
        container_resources=container_resources,
        is_delete_operator_pod=False,
        tolerations=gpu_tolerations,
        startup_timeout_seconds=600,
        # node_selector=gpu_node_selector,
        **storage.kpo_kwargs(),
    )

    read_task = CustomKubernetesPodOperator(
        task_id="read_task",
        runtime_mode="containerized",
        private_registry=False,
        runtime_image="python",
        runtime_image_tag="3.12-slim",
        cmds=["sh", "-c"],
        arguments=[f"echo '{encode_script(READ_SCRIPT)}' | base64 -d | python"],
        container_resources=container_resources,
        is_delete_operator_pod=False,
        tolerations=gpu_tolerations,
        # node_selector=gpu_node_selector,
        priority_weight=1000,  # don't let the node scale down while your task is enqueued
        **storage.kpo_kwargs(),
    )

    storage.setup >> write_task >> read_task >> storage.teardown
