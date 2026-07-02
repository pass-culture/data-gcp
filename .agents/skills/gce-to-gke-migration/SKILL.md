---
name: gce-to-gke-migration
description: 'Migrate Airflow DAGs from GCE (SSHGCEOperator) to GKE (CustomKubernetesPodOperator). Use when the user wants to migrate a DAG from VM-based to Kubernetes-based execution, replace SSHGCEOperator with CustomKubernetesPodOperator, convert GCE tasks to KPO tasks, or mentions GCE-to-GKE migration. Also trigger when they mention removing StartGCEOperator, DeleteGCEOperator, InstallDependenciesOperator in favor of pod-based execution.'
argument-hint: 'Path to the DAG file to migrate, or DAG name'
---

# GCE to GKE Migration

Automates the migration of Airflow DAGs from GCE architecture (SSHGCEOperator + VM lifecycle) to GKE architecture (CustomKubernetesPodOperator).

## When to Use

- User wants to migrate a DAG from `SSHGCEOperator` to `CustomKubernetesPodOperator`
- User mentions GCE-to-GKE, VM-to-pod, or Kubernetes migration
- User wants to remove VM lifecycle operators (StartGCE, DeleteGCE, InstallDependencies)

## Prerequisites

Before starting, confirm with the user:
1. Which DAG file(s) to migrate
2. Whether to use `gitsynced` or `containerized` runtime mode (default: `gitsynced` for ETL jobs)
3. Whether custom resources (CPU/RAM/GPU) are needed

## Migration Procedure

### Step 1: Analyze the Existing DAG

1. Read the microservice path `BASE_PATH` and analyze the job to define if it writes data in VM disk. If so, tasks need to share a volume (dynamic PVC) between sequential tasks. Use the lifecycle the advanced patterns reference for this (Shared Volumes Between Sequential Tasks).

2. Read the target DAG file and identify:
- All `SSHGCEOperator` tasks and their parameters (`command`, `base_dir`, `environment`)
- VM lifecycle operators: `StartGCEOperator`, `DeleteGCEOperator`, `InstallDependenciesOperator`
- The `GCE_INSTANCE` name and `BASE_PATH` (maps to `microservice_path`)
- Task dependencies chain
- Any `dag_config` environment variables passed


### Step 2: Present Migration Plan

Show the user a summary of changes before applying:

```
Migration Plan for: <DAG_NAME>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REMOVE:
  - StartGCEOperator (gce_start_task)
  - InstallDependenciesOperator (fetch_install_code)
  - DeleteGCEOperator (gce_stop_task)

ADD:
  - Storage Lifecycle (dynamic PVC) if tasks share data via filesystem. For example, storage = make_storage_lifecycle(storage_size="10Gi")

CONVERT (SSHGCEOperator → CustomKubernetesPodOperator):
  - <task_id>: command="<cmd>" → arguments=["<cmd_parts>"]

SETTINGS:
  - runtime_mode: gitsynced
  - runtime_image: py3XX
  - microservice_path: <derived from BASE_PATH>
  - orchestration_mode: celery (default)

TAG CHANGE: DAG_TAGS.VM → DAG_TAGS.POD
```

**Wait for user approval before proceeding.**

### Step 3: Apply Migration

After user approval, apply these transformations:

#### 3a. Update Imports

Remove:
```python
from common.operators.gce import (
    SSHGCEOperator,
    StartGCEOperator,
    DeleteGCEOperator,
    InstallDependenciesOperator,
)
```

Add:
```python
from common.operators.kubernetes import (
    CustomKubernetesPodOperator,
    DEFAULT_CONTAINER_RESOURCES,
)
```

Also remove `from common.callback import on_failure_vm_callback` if present (not needed for KPO).

#### 3b. Remove GCE-Specific Variables

Remove:
- `GCE_INSTANCE = f"..."` variable
- `BASE_PATH` variable (convert to `microservice_path` format)

Add:
- `MICROSERVICE_PATH` — derived from `BASE_PATH` by removing the leading repo name prefix (e.g., `data-gcp/jobs/etl_jobs/external/adage` → `jobs/etl_jobs/external/adage`)

#### 3c. Convert SSHGCEOperator Tasks

For each `SSHGCEOperator` task, convert to `CustomKubernetesPodOperator`:

**Before (GCE):**
```python
task = SSHGCEOperator(
    task_id="my_task",
    instance_name=GCE_INSTANCE,
    base_dir=BASE_PATH,
    environment=dag_config,
    command="uv run main.py --arg1 value1",
)
```

**After (KPO):**
```python
task = CustomKubernetesPodOperator(
    task_id="my_task",
    orchestration_mode="celery",
    queue="k8s-watcher",
    runtime_mode="gitsynced",
    runtime_branch="{{ params.branch }}",
    runtime_image="py313",
    runtime_image_tag="v1",
    microservice_path=MICROSERVICE_PATH,
    arguments=["main.py", "--arg1", "value1"],
    container_resources=DEFAULT_CONTAINER_RESOURCES,
)
```

**Conversion rules for `arguments`:**
- The `command` field typically looks like `uv run main.py --flag value`. Strip the `uv run` prefix (handled by the operator in gitsynced mode).
- Split the remaining command into a list: `["main.py", "--flag", "value"]`
- Preserve Jinja templates as-is in the list elements
- If environment variables were passed via `environment=dag_config`, note that `GCP_PROJECT_ID` and `ENV_SHORT_NAME` are already injected by the KPO operator by default — only add extra env vars if needed via `env_vars`

#### 3d. Remove VM Lifecycle Operators

Delete:
- `StartGCEOperator` task
- `InstallDependenciesOperator` task
- `DeleteGCEOperator` task

#### 3e. Update Task Dependencies

Remove VM lifecycle from the chain.

**Before:**
```python
gce_start >> fetch_install >> task1 >> task2 >> gce_stop >> end
```

**After:**
```python
task1 >> task2 >> end
```

In case of shared volume (dynamic PVC), ensure the storage lifecycle tasks are added and dependencies updated accordingly. For example,
**Before:**
```python
gce_start >> fetch_install >> write_task >> read_task >> gce_stop >> end
```

**After:**
```python
storage = make_storage_lifecycle(storage_size="10Gi")
storage.setup >> write_task >> read_task >> storage.teardown
````


#### 3f. Update DAG Metadata

- Change `tags`: replace `DAG_TAGS.VM.value` with `DAG_TAGS.POD.value`
- Remove `on_failure_callback=on_failure_vm_callback` from `default_dag_args` (replace with `task_fail_slack_alert` if not already present)
- Keep `params.branch` (used by `runtime_branch`)

#### 3g. Patch `main.py` Error Handling (Critical — Automated)

Open the microservice's `main.py` (path derived from `microservice_path`). If it uses Typer, **automatically patch** the main function to propagate errors correctly.

**Detection:** Look for `import typer` or `typer.Typer()` usage. If found, check whether the main command function already has a `try/except` block that re-raises as `typer.Exit(code=1)`.

**If not already wrapped**, add the error-handling wrapper around the function body:

```python
import typer

def main(etl_parameters...):
    try:
        # existing etl workflow code (indent +1 level)
    except typer.Exit:
        # typer.Exit is a subclass of Exception — re-raise before broad except catches it
        raise
    except Exception as e:
        logger.exception(f"ETL job failed: {e}")
        raise typer.Exit(code=1) from e
```

**Steps:**
1. Read `<microservice_path>/main.py`
2. Identify the Typer command function (decorated with `@app.command()` or the main callable)
3. If the function body is NOT already wrapped in a try/except with `typer.Exit(code=1)`:
   - Wrap the entire function body in `try:`
   - Add `except typer.Exit: raise` clause
   - Add `except Exception as e: logger.exception(...); raise typer.Exit(code=1) from e` clause
   - Ensure `import logging` and `logger = logging.getLogger(__name__)` exist (add if missing)
4. Show the diff to the user and **wait for approval** before saving

**Also patch `pyproject.toml`:** Read `<microservice_path>/pyproject.toml` and check the `typer` dependency version. If it is pinned below 0.24 (e.g., `typer>=0.12` or `typer<0.24`), update it to `typer>=0.24`. If typer is not listed, add it and regenerate the lock file (cd to microservice path, run `uv sync`). Warn the user that below 0.24, argument parsing is incompatible with the container entrypoint — they should test the CLI after bumping.

### Step 4: Validate

After migration:
1. Check for syntax errors in the modified DAG
2. Verify no remaining references to GCE operators or variables
3. Confirm task dependency chain is valid
4. If changes have been applied in `<microservice_path>`, remind the user to push their microservice code to the branch used by `runtime_branch`.
5. Test the new DAG in a testing environment before deploying to production. Use `make test_dag` and `make push_dag` in `orchestration/Makefile`.

## Advanced Use Cases

See [./references/advanced-patterns.md](./references/advanced-patterns.md) for:
- Custom CPU/RAM resources
- Shared volumes between tasks (dynamic PVC)
- GPU workloads
- Containerized mode for heavy dependencies

## Common Pitfalls

| Pitfall | Fix |
|---------|-----|
| Typer swallows exceptions silently | Wrap in try/except, re-raise as `typer.Exit(code=1)` |
| Typer < 0.24 breaks argument parsing | Pin `typer>=0.24` in `pyproject.toml` | regenerate uv lock file (cd to microservice path, run `uv sync`) |
| Forgot to push microservice code | `runtime_branch` fetches code at runtime — branch must exist |
| Using `image_pull_policy="IfNotPresent"` | Always use default (Always) to avoid stale images |
| Templating `orchestration_mode` via params | Not possible — scheduler needs it at parse time |
| Binding volume to parallel tasks | PVCs are ReadWriteOnce — only sequential access |
