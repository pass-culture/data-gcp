import base64
import datetime

from airflow import DAG
from common.dynamic_pvc import make_storage_lifecycle
from common.operators.kubernetes import EasyKubernetesPodOperator
from kubernetes.client import V1ResourceRequirements

DOC = """
## Dynamic PVC provisioning — example DAG

Demonstrates how to share data between KPO tasks using a dynamically provisioned
PersistentVolumeClaim (standard-rwo / GKE Persistent Disk).

```
storage.setup → write_task → read_task → storage.teardown
```

### Design constraints

- **ReadWriteOnce**: tasks sharing the volume must run sequentially.
- **max_active_runs=1**: PVC name is fixed at parse time as
  `airflow-{env}-pvc-{dag_id}`. Concurrent runs would share the same PVC.
- **ReclaimPolicy Delete**: teardown deletes the PVC and underlying GCE disk.

### Usage pattern

Use ``make_storage_lifecycle(storage_size=...)`` to get setup/teardown TaskGroups
and inject ``**storage.kpo_kwargs()`` into any operator that needs the volume.
"""

DAG_ID = "kpo_shared_volume_example_v3"
MOUNT_PATH = "/shared"


def encode_script(script: str) -> str:
    return base64.b64encode(script.encode()).decode()


WRITE_SCRIPT = f"""
import json, datetime

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
    description="Demo: dynamic PVC provisioning (standard-rwo) with write/read KPO tasks",
    doc_md=DOC,
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

    write_task = EasyKubernetesPodOperator(
        task_id="write_task",
        runtime_mode="containerized",
        private_registry=False,
        runtime_image="python",
        runtime_image_tag="3.12-slim",
        cmds=["sh", "-c"],
        arguments=[f"echo '{encode_script(WRITE_SCRIPT)}' | base64 -d | python"],
        container_resources=resources,
        is_delete_operator_pod=False,
        volumes=[
            volume
        ],  # we could also use **storage.kpo_kwargs() here, but we want to demonstrate the explicit volume/mount usage
        volume_mounts=[mount],  # same as above
    )

    read_task = EasyKubernetesPodOperator(
        task_id="read_task",
        runtime_mode="containerized",
        private_registry=False,
        runtime_image="python",
        runtime_image_tag="3.12-slim",
        cmds=["sh", "-c"],
        arguments=[f"echo '{encode_script(READ_SCRIPT)}' | base64 -d | python"],
        container_resources=resources,
        is_delete_operator_pod=False,
        **storage.kpo_kwargs(),  # convenience wrapper that injects the same volume/mount info as above
    )

    storage.setup >> write_task >> read_task >> storage.teardown
