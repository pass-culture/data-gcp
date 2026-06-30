# Advanced Migration Patterns

## Custom CPU/RAM Resources

When the microservice needs more than default resources (0.2 CPU / 500Mi RAM):

```python
from kubernetes.client import V1ResourceRequirements

container_resources = V1ResourceRequirements(
    requests={"cpu": "0.2", "memory": "500Mi"},
    limits={"cpu": "0.5", "memory": "1Gi"},
)

task = CustomKubernetesPodOperator(
    ...,
    container_resources=container_resources,
)
```

## Shared Volumes Between Sequential Tasks

When tasks in the same DAG need to share data via filesystem:

**Constraints:**
- PVCs are ReadWriteOnce — only one pod at a time can bind
- Requires `max_active_runs=1` on the DAG
- Tasks sharing the volume must be sequential (not parallel)

```python
from common.dynamic_pvc import make_storage_lifecycle

storage = make_storage_lifecycle(storage_size="10Gi")

task1 = CustomKubernetesPodOperator(
    task_id="write_task",
    ...,
    **storage.kpo_kwargs(),  # injects volumes + volume_mounts
)

task2 = CustomKubernetesPodOperator(
    task_id="read_task",
    ...,
    **storage.kpo_kwargs(),
)

# Dependencies MUST include setup/teardown
storage.setup >> task1 >> task2 >> storage.teardown
```

## GPU Workloads

For tasks requiring GPU (e.g., ML training/inference):

```python
from kubernetes.client import V1ResourceRequirements, V1Toleration

gpu_node_selector = {
    "cloud.google.com/gke-nodepool": "data-gpu-node-pool-n1-standard-8"
}

gpu_tolerations = [
    V1Toleration(
        key="gpu-n1-s-8",
        operator="Equal",
        value="true",
        effect="NoSchedule",
    ),
    V1Toleration(
        key="nvidia.com/gpu",
        operator="Equal",
        value="present",
        effect="NoSchedule",
    ),
]

container_resources = V1ResourceRequirements(
    requests={"cpu": "6", "memory": "20Gi"},
    limits={"nvidia.com/gpu": "1"},
)

gpu_task = CustomKubernetesPodOperator(
    ...,
    container_resources=container_resources,
    startup_timeout_seconds=600,  # GPU node needs time to boot + install drivers
    tolerations=gpu_tolerations,
    node_selector=gpu_node_selector,
)
```

## Containerized Mode

Use when dependencies are heavy (e.g., PyTorch) and code is stable:

```python
task = CustomKubernetesPodOperator(
    task_id="ml_inference",
    runtime_mode="containerized",
    runtime_image="my-ml-image",
    runtime_image_tag="v1",
    private_registry=True,  # image is in our artifact registry
    arguments=["main.py", "--model", "production"],
    container_resources=container_resources,
)
```

**When to use containerized vs gitsynced:**

| Criteria | gitsynced | containerized |
|----------|-----------|---------------|
| Dependencies | Light (fast `uv install`) | Heavy (PyTorch, etc.) |
| Code stability | Active development | Stable in production |
| Cold start | Slightly slower (clone + install) | Fast (pre-built) |
| Iteration speed | High (no image rebuild) | Low (requires rebuild) |

## Orchestration Modes

| Mode | Worker | Use When |
|------|--------|----------|
| `celery` (default) | Celery worker dispatches pod | Most jobs, simpler setup |
| `kubernetes` | KubernetesExecutor creates pod directly | High-parallelism DAGs, resource isolation |

For `celery` mode, always set `queue="k8s-watcher"`.

## Environment Variables

The KPO operator automatically injects:
- `GCP_PROJECT_ID`
- `ENV_SHORT_NAME`
- `UV_CACHE_DIR`
- `UV_LINK_MODE`
- `RUN_ID`

To add custom env vars:
```python
task = CustomKubernetesPodOperator(
    ...,
    env_vars={
        "MY_CUSTOM_VAR": "value",
        "ANOTHER_VAR": "{{ ds }}",  # Jinja templates work
    },
)
```
