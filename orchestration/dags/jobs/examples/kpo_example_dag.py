import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    GCP_PROJECT_ID,
)
from common.operators.kubernetes import CustomKubernetesPodOperator
from kubernetes.client import V1ResourceRequirements

DOC = """
## CustomKubernetesPodOperator — example DAG

Exercises all four combinations of `orchestration_mode x runtime_mode` exposed by
`CustomKubernetesPodOperator`. Use this DAG to validate operator wiring before promoting
a configuration to a production DAG.

---

### orchestration_mode — who schedules the job pod

Controls which pod calls the Kubernetes API to create the actual job pod.

**`celery`** (default)
- Routes to the default Airflow celery worker (default queue).
- Celery workers are autoscaled by KEDA based on the number of pending tasks in the queue and the number of available slots in workers.
- using queue="k8s-watcher", routes to a dedicated worker set whith higher concurrency and it own keda autoscaling.
- Worker pod is fully defined by the Airflow Helm chart — the operator adds nothing.

**`kubernetes`**
- Routes to the `kubernetes` queue.
- The Helm chart pod template is used as the base, with a `git-sync` init container
  injected to clone the DAGs repo into `/opt/airflow/dags` before the worker starts.
- `dag_branch` *(literal string — default: `"master"` / `"production"` depending on env)*:
  branch of the DAGs repo cloned into the worker pod.
- `dag_image_tag` *(literal string — default: `"dev"` in dev, `"v1"` otherwise)*:
  image tag for the Airflow worker container (`airflow-k8s-worker:<tag>`).
- **Both must be literal strings.** They are embedded into `executor_config` at DAG
  parse time — before the worker pod exists and before any Jinja rendering. Jinja
  templates, XCom refs, and `params` cannot be used here. To switch branches or tags,
  update the value in the DAG file directly.

---

### runtime_mode — how the job pod gets its code

Controls the image and entrypoint of the actual job pod.

**`gitsynced`**
- Job pod uses a base Python image (`py312:<runtime_image_tag>` by default).
- A `git-sync` init container clones the microservice repo at `runtime_branch` into `/app`.
- The operator constructs the entrypoint: `sh -c "cd /app && uv run <arguments>"`.
- `runtime_branch` *(templated, required)*: branch of the microservice repo to clone.
- `microservice_path` *(required)*: path inside the repo copied into `/app`;
  must contain a `pyproject.toml` (e.g. `jobs/etl_jobs/external/instagram`).
- `runtime_image` *(templated, optional — default: `py312`)*: override the base Python image.
- `runtime_image_tag` *(templated — default: `"dev"` in dev, `"v1"` otherwise)*: image tag.
- `arguments` *(list)*: split list of script name + flags, e.g.
  `["main.py", "--start-date", "2024-01-01"]`. Do not include `uv run` — the operator
  joins the items with spaces and prepends `cd /app && uv run` automatically.
  **Limitation**: argument values that contain spaces will be word-split by the shell;
  avoid spaces in values or wrap them in shell quotes.

**`containerized`**
- Job pod uses a fully self-contained image — code and deps are pre-installed.
- No init container; image ENTRYPOINT is preserved.
- `runtime_image` *(required)*: image name. Behaviour depends on `private_registry`:
  - `True` (default): short name (e.g. `"etl/instagram"`) — internal Artifact Registry
    prefix is prepended automatically. Pass the full registry URL to skip prefixing.
  - `False`: used as-is (e.g. `"python"`, `"alpine"`) — no registry prefix added.
- `runtime_image_tag` *(templated — default: `"dev"` in dev, `"v1"` otherwise)*: appended
  to the image ref as the tag.
- `arguments` *(list)*: split list passed as individual argv elements to the ENTRYPOINT,
  e.g. `["main.py", "--flag", "value"]`. Same format as `gitsynced` — both modes accept
  the identical argument list.

---

### What happens under the hood — per task

**celery_gitsynced**
```
Celery worker (helm chart, untouched)
  └─ KPO.execute() → job pod
       ├─ init: git-sync  →  git clone --branch <runtime_branch> .../instagram  →  /app
       └─ main: py312:<runtime_image_tag>
                sh -c "cd /app && uv run main.py --start-date ... --end-date ..."
```

**celery_containerized**
```
Celery worker (helm chart, untouched)
  └─ KPO.execute() → job pod
       └─ main: <registry>/etl/instagram:<runtime_image_tag>
                ENTRYPOINT uv run  +  CMD ["main.py", "--start-date","...","--end-date","..."]
```

**k8s_gitsynced**
```
Kubernetes executor → worker pod  [dag_branch + dag_image_tag baked in at parse time]
  ├─ init: git-sync  →  git clone --branch <dag_branch> .../data-gcp  →  /opt/airflow/dags
  └─ main: airflow-k8s-worker:<dag_image_tag>
             └─ KPO.execute() → job pod
                  ├─ init: git-sync  →  git clone --branch <runtime_branch> .../instagram  →  /app
                  └─ main: py312:<runtime_image_tag>
                           sh -c "cd /app && uv run main.py --start-date ... --end-date ..."
```

**k8s_containerized**
```
Kubernetes executor → worker pod  [dag_branch + dag_image_tag baked in at parse time]
  ├─ init: git-sync  →  git clone --branch <dag_branch> .../data-gcp  →  /opt/airflow/dags
  └─ main: airflow-k8s-worker:<dag_image_tag>
             └─ KPO.execute() → job pod
                  └─ main: <registry>/etl/instagram:<runtime_image_tag>
                           ENTRYPOINT uv run  +  CMD ["main.py", "--start-date","...","--end-date","..."]
```

---

### Common parameters (all overridable per task)

- `namespace` — default `airflow-<env>`, derived from `ENVIRONMENT_NAME`
- `service_account_name` — default `"airflow-worker"`
- `in_cluster` — auto: `True` unless `LOCAL_ENV` is set
- `image_pull_policy` — auto: `"Always"` in dev, `"IfNotPresent"` otherwise
- `get_logs` — default `True`
- `is_delete_operator_pod` — default `True`
- `on_finish_action` — default `"delete_pod"`
- `kubernetes_conn_id` — auto: `"kubernetes_default"` when running locally
- `private_registry` *(containerized mode only, default `True`)*: controls image ref construction.
  `True` — prepends the internal Artifact Registry prefix to `runtime_image` (e.g. `"etl/instagram"`
  becomes `<registry>/data-gcp/etl/instagram:<tag>`). Pass the full registry URL to skip prefixing.
  `False` — `runtime_image` is used as-is, no prefix added (e.g. `"python"` → `"python:<tag>"`).
  Use `False` for public Docker Hub or external images.

---

### Known limitations

Using celery orchestration mode, occupies a celery worker (autoscaled by keda).
This means that if the job pod is long-running, the worker will be occupied for the duration and unavailable to run other tasks.
In contrast, using kubernetes orchestration mode, the worker pod belong to is own queue and will not be limited by keda autoscaling.

Deferrable mode (deferrable = True, poll_interval= 300s) is available for both orchestration modes, however, logs can only be retrieved at the end of the task execution (kubernetes), and not streamed in real time. This is a limitation of the underlying KubernetesPodOperator when used in deferrable mode, and applies to both orchestration modes.

---

### Advices and Troubleshooting

when containering a microservice, it is recommended to add ENTRYPOINT ["uv","run"] in the Dockerfile, so that you can pass the script name and arguments as CMD and have a consistent experience with the gitsynced runtime, which also uses `uv run` as the entrypoint command. This way, you can switch between runtimes without changing the way you pass arguments.
when using kubernetes orchestration mode, make sure to push your DAG changes to the branch set in `dag_branch` (hardcoded literal in the DAG file, defaults to `master`/`production`), as the worker pod clones that branch and runs Airflow from that code. If you are only iterating on the microservice and the DAG arguments are unchanged between runs, you can leave `dag_branch` and `dag_image_tag` as-is and only update `runtime_branch`, which controls what code the job pod runs.
Developement workflow is recommended to be done using git-sync with a base image as you don't need to build a new image at each change, and you can easily access the logs in real time to debug. Once the code is stable, you can switch to containerized mode for faster execution and better resource management.
Deferrable mode is not available when running airflow locally with docker-compose due to the absence of triggerer component.
when running airflow locally, only the runtime worker pod is created on the cluster, the operator runs in the local environment and communicates with the cluster to create the job pod. In this case, make sure to have the right kubeconfig context set up to point to the desired cluster.

"""
MICROSERVICE_PATH = "jobs/etl_jobs/external/instagram"

DAG_NAME = "kpo_example_dag"

branch = "docker-security"  # "master" if ENV_SHORT_NAME != "prod" else "production"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "project_id": GCP_PROJECT_ID,
}


# Split-list format works for both runtime modes:
#   - containerized: each item is passed as a separate argv element to the ENTRYPOINT.
#   - gitsynced: items are joined with spaces into a single shell string by the operator,
#     then executed as: sh -c "cd /app && uv run main.py --flag value ..."
# Limitation: argument values that contain spaces will be word-split by the shell in
# gitsynced mode. Avoid spaces in values, or wrap them in shell quotes if needed.
_ARGS = [
    "main.py",
    "--start-date",
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_days) }}",
    "--end-date",
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}{{ add_days(base, params.n_index) }}",
]

# Custom resources and env vars for all tasks in this DAG, on top of the defaults defined in KPO_COMMON_DEFAULTS.
container_resources = V1ResourceRequirements(
    requests={"cpu": "0.2", "memory": "1Gi"},
    limits={"cpu": "0.5", "memory": "1Gi"},
)
extra_env_vars = {
    "DUMMY_ENV_VAR": "dummy_value"
}  # this is just an example of how to add extra env vars on top of the defaults provided by the operator

kpo_common = {}
kpo_common["container_resources"] = container_resources
kpo_common["env_vars"] = extra_env_vars


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="CustomKubernetesPodOperator — all four mode combinations",
    doc_md=DOC,
    schedule_interval=None,
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=60),
    params={
        "runtime_branch": Param(
            default=branch,
            type="string",
            description="Git branch for the job pod (gitsynced runtime)",
        ),
        "runtime_image_tag": Param(
            default="dev",
            type="string",
            description="Tag for the runtime job image (py31x for gitsynced, user image for containerized)",
        ),
        "n_days": Param(
            default=-7,
            type="integer",
            description="Days before execution date for start date",
        ),
        "n_index": Param(
            default=0,
            type="integer",
            description="Offset from execution date for end date",
        ),
    },
    tags=["example", "kubernetes"],
):
    # ── celery + gitsynced ──────────────────────────────────────────────────
    CustomKubernetesPodOperator(
        task_id="celery_gitsynced",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_image="py310",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        runtime_branch="{{ params.runtime_branch }}",
        microservice_path=MICROSERVICE_PATH,
        arguments=_ARGS,
        **kpo_common,
    )

    # ── celery + containerized ──────────────────────────────────────────────
    CustomKubernetesPodOperator(
        task_id="celery_containerized",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="containerized",
        runtime_image="etl/instagram",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        arguments=_ARGS,
        **kpo_common,
    )

    # ── kubernetes + gitsynced ──────────────────────────────────────────────
    CustomKubernetesPodOperator(
        task_id="k8s_gitsynced",
        orchestration_mode="kubernetes",
        dag_branch=branch,
        runtime_mode="gitsynced",
        runtime_image="py310",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        runtime_branch="{{ params.runtime_branch }}",
        microservice_path=MICROSERVICE_PATH,
        arguments=_ARGS,
        # deferrable=True,
        # poll_interval=60,
        **kpo_common,
    )

    # ── kubernetes + containerized ──────────────────────────────────────────
    CustomKubernetesPodOperator(
        task_id="k8s_containerized",
        orchestration_mode="kubernetes",
        dag_branch=branch,
        runtime_mode="containerized",
        runtime_image="etl/instagram",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        arguments=_ARGS,
        # deferrable=True,
        # poll_interval=60,
        **kpo_common,
    )
