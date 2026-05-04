import base64
import datetime

from airflow import DAG
from common.config import GCS_AIRFLOW_BUCKET
from common.operators.kubernetes import EasyKubernetesPodOperator
from common.shared_volume import DEFAULT_PVC_NAME, make_storage_lifecycle
from kubernetes.client import V1ResourceRequirements

DOC = """
## Shared GCS FUSE volume — example DAG

Demonstrates how to share data between KPO tasks within the same DAG run using a
GCS FUSE-backed PersistentVolumeClaim. A `write_task` writes a JSON file to the
shared volume; a `read_task` reads it back and prints the content.

---

### How the shared volume works

The PV and PVC are provisioned permanently via Helm. This DAG only manages the
**GCS folder lifecycle**: `storage_setup` creates a scoped folder before the tasks
run; `storage_teardown` deletes it after.

```
storage_setup → write_task → read_task → storage_teardown
```

Volume scoping is handled in two layers:

| Layer | Mechanism | Scope |
|---|---|---|
| DAG-level | `sub_path` in `V1VolumeMount` | `shared/{dag_id}/` — isolates DAGs from each other |
| Run-level | `RUN_ID` env var + run-scoped teardown | `{mount_path}/{RUN_ID}/` — isolates concurrent runs within the same DAG |

`sub_path` is resolved at DAG parse time (no Jinja). `RUN_ID` is rendered at
execution time via env var — that is why the scripts write to `/shared/{RUN_ID}/`
rather than a fixed path. Setup and teardown are both scoped to the run folder
(`shared/{dag_id}/{run_id}/`), so concurrent runs are fully independent.

---

### `storage.kpo_kwargs()`

`make_storage_lifecycle` returns a `StorageLifecycle` object. Spread
`**storage.kpo_kwargs()` into every operator that needs the shared volume — it
injects all of the following in one call:

- `volumes` / `volume_mounts` / `annotations` — GCS FUSE volume wiring
- `RUN_ID` — Jinja-rendered run ID for per-run file isolation
- `GCS_FOLDER` — resolved GCS path for the current run (`shared/{dag_id}/{run_id}`)

Pass `extra_env_vars` to merge additional variables without losing the defaults.

"""

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
    doc_md=DOC,
    schedule_interval=None,
    catchup=False,
    tags=["example", "kubernetes"],
):
    storage = make_storage_lifecycle(
        bucket_name=GCS_AIRFLOW_BUCKET,
        pvc_name=DEFAULT_PVC_NAME,
    )

    write_task = EasyKubernetesPodOperator(
        task_id="write_task",
        runtime_mode="containerized",
        private_registry=False,
        runtime_image="python",
        runtime_image_tag="3.12-slim",
        cmds=["sh", "-c"],
        arguments=[f"echo '{encode_script(WRITE_SCRIPT)}' | base64 -d | python"],
        container_resources=resources,
        **storage.kpo_kwargs(),
    )

    read_task = EasyKubernetesPodOperator(
        task_id="read_task",
        runtime_mode="containerized",
        runtime_image="python",
        runtime_image_tag="3.12-slim",
        private_registry=False,
        cmds=["sh", "-c"],
        arguments=[f"echo '{encode_script(READ_SCRIPT)}' | base64 -d | python"],
        container_resources=resources,
        **storage.kpo_kwargs(),
    )

    storage.setup >> write_task >> read_task >> storage.teardown
