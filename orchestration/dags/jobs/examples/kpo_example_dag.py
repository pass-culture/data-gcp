import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.kubernetes import EasyKubernetesPodOperator
from kubernetes.client import V1ResourceRequirements

DOC = """
## EasyKubernetesPodOperator — example DAG

Exercises all four combinations of `orchestration_mode x runtime_mode` exposed by
`EasyKubernetesPodOperator`. Use this DAG to validate operator wiring before promoting
a configuration to a production DAG.

---

### orchestration_mode — who schedules the job pod

Controls which pod calls the Kubernetes API to create the actual job pod.

**`celery`** (default)
- Routes to the default Airflow celery worker queue.
- Worker pod is fully defined by the Airflow Helm chart — the operator adds nothing.

**`kubernetes`**
- Routes to the `kubernetes` queue.
- The Helm chart pod template is used as the base, with a `git-sync` init container
  injected to clone the DAGs repo into `/opt/airflow/dags` before the worker starts.
- `dag_branch` *(templated — default: `"master"` / `"production"` depending on env)*: branch of the DAGs
  repo to clone into the worker pod.
- `dag_image_tag` *(templated — default: `"dev"`)*: image tag for the Airflow worker
  container (`airflow-k8s-worker:<tag>`).
- Both params are resolved in `execute()` after Jinja rendering, so
  `{{ params.xxx }}` works for both.

---

### runtime_mode — how the job pod gets its code

Controls the image and entrypoint of the actual job pod.

**`gitsynced`**
- Job pod uses a base Python image (`py31x:<runtime_image_tag>`).
- A `git-clone` init container clones the microservice repo at `branch` into `/app`.
- The operator constructs the entrypoint: `sh -c "cd /app && uv run <arguments>"`.
- `runtime_branch` *(templated, required)*: branch of the microservice repo to clone.
- `microservice_path` *(required)*: path inside the repo copied into `/app`;
  must contain a `pyproject.toml` (e.g. `jobs/etl_jobs/external/instagram`).
- `runtime_image_tag` *(templated — default: `"dev"`)*: tag of the base Python image.
- `arguments` *(list)*: script name + flags as a **single shell string**,
  e.g. `["main.py --start-date 2024-01-01"]`. Do not include `uv run` — the operator
  prepends it automatically.

**`containerized`**
- Job pod uses a fully self-contained image — code and deps are pre-installed.
- No init container; image ENTRYPOINT is preserved.
- `image` *(required)*: fully qualified image reference including tag.
- `arguments` *(list)*: passed directly to the image ENTRYPOINT as separate args.
  Use a **split list** `["--flag", "value"]` — a single concatenated string is treated
  as one argument by the container runtime and will be rejected by most CLIs.

---

### What happens under the hood — per task

**celery_gitsynced**
```
Celery worker (helm chart, untouched)
  └─ KPO.execute() → job pod
       ├─ init: git-clone  →  git clone --branch <branch> .../instagram  →  /app
       └─ main: py31x:<runtime_image_tag>
                sh -c "cd /app && uv run main.py --start-date ... --end-date ..."
```

**celery_containerized**
```
Celery worker (helm chart, untouched)
  └─ KPO.execute() → job pod
       └─ main: etl/instagram:<runtime_image_tag>
                ENTRYPOINT uv run  +  CMD ["main.py", "--start-date","...","--end-date","..."]
```

**k8s_gitsynced**
```
Kubernetes executor → worker pod
  ├─ init: git-sync  →  git clone --branch <dag_branch> .../data-gcp  →  /opt/airflow/dags
  └─ main: airflow-k8s-worker:<dag_image_tag>
             └─ KPO.execute() → job pod
                  ├─ init: git-clone  →  git clone --branch <runtime_branch> .../instagram  →  /app
                  └─ main: py31x:<runtime_image_tag>
                           sh -c "cd /app && uv run main.py --start-date ... --end-date ..."
```

**k8s_containerized**
```
Kubernetes executor → worker pod
  ├─ init: git-sync  →  git clone --branch <dag_branch> .../data-gcp  →  /opt/airflow/dags
  └─ main: airflow-k8s-worker:<dag_image_tag>
             └─ KPO.execute() → job pod
                  └─ main: etl/instagram:<runtime_image_tag>
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

---

### Known limitations

Using celery orchestration mode, occupies a celery worker (autoscaled by keda).
This means that if the job pod is long-running, the worker will be occupied for the duration and unavailable to run other tasks.
In contrast, using kubernetes orchestration mode, the worker pod belong to is own queue and will not be limited by keda autoscaling.

Deferrable mode (deferrable = True, poll_interval= 300s) is available for both orchestration modes, however, logs can only be retrieved at the end of the task execution (kubernetes), and not streamed in real time. This is a limitation of the underlying KubernetesPodOperator when used in deferrable mode, and applies to both orchestration modes.

---

### Advices and Troubleshooting

when containering a microservice, it is recommended to add ENTRYPOINT ["uv","run"] in the Dockerfile, so that you can pass the script name and arguments as CMD and have a consistent experience with the gitsynced runtime, which also uses `uv run` as the entrypoint command. This way, you can switch between runtimes without changing the way you pass arguments.
when using kubernetes orchestration mode, make sure to push your dags change to the branch specified in `dag_branch` param, as the worker pod will clone the DAGs repo and run airflow commands based on that code. If you are testing the microservice and DAG argument do not change between runs, you can use the same worker pod by not changing the `dag_branch` and focus only on `runtime_branch` param which controls the microservice code.
Developement workflow is recommended to be done using git-sync with a base image as you don't need to build a new image at each change, and you can easily access the logs in real time to debug. Once the code is stable, you can switch to containerized mode for faster execution and better resource management.
Deferrable mode is not available when running airflow locally with docker-compose due to the absence of triggerer component.
when running airflow locally, only the runtime worker pod is created on the cluster, the operator runs in the local environment and communicates with the cluster to create the job pod. In this case, make sure to have the right kubeconfig context set up to point to the desired cluster.

"""
MICROSERVICE_PATH = "jobs/etl_jobs/external/instagram"

DAG_NAME = "kpo_example_dag"

branch = "master" if ENV_SHORT_NAME != "prod" else "production"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "project_id": GCP_PROJECT_ID,
}


# For gitsynced: single shell string — operator wraps in `sh -c`.
_ARGS_SHELL = (
    "main.py "
    "--start-date "
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}"
    "{{ add_days(base, params.n_days) }} "
    "--end-date "
    "{% set base = yesterday() if dag_run.run_type == 'manual' else ds %}"
    "{{ add_days(base, params.n_index) }}"
)

# For containerized: split list — each item becomes a separate arg to the image ENTRYPOINT.
_ARGS_LIST = [
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
    description="EasyKubernetesPodOperator — all four mode combinations",
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
        "dag_branch": Param(
            default=branch,
            type="string",
            description="DAGs repo branch cloned into the kubernetes executor worker pod",
        ),
        "dag_image_tag": Param(
            default="dev",
            type="string",
            description="Tag for the Airflow worker image used by the kubernetes executor worker pod",
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
    # Operator forces `sh -c` and prepends `cd /app && uv run `.
    # Pass the script name + args as a single shell string.
    EasyKubernetesPodOperator(
        task_id="celery_gitsynced",
        runtime_mode="gitsynced",
        runtime_image="py310",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        runtime_branch="{{ params.runtime_branch }}",
        microservice_path=MICROSERVICE_PATH,
        arguments=[_ARGS_SHELL],
        **kpo_common,
    )

    # ── celery + containerized ──────────────────────────────────────────────
    # Image ENTRYPOINT is preserved. Pass each flag/value as a separate list
    # item so Kubernetes delivers them as individual args to the entrypoint.
    EasyKubernetesPodOperator(
        task_id="celery_containerized",
        runtime_mode="containerized",
        runtime_image="etl/instagram",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        arguments=_ARGS_LIST,
        **kpo_common,
    )

    # ── kubernetes + gitsynced ──────────────────────────────────────────────
    # Operator forces `sh -c` and prepends `cd /app && uv run `.
    # Pass the script name + args as a single shell string.
    EasyKubernetesPodOperator(
        task_id="k8s_gitsynced",
        orchestration_mode="kubernetes",
        runtime_mode="gitsynced",
        runtime_image="py310",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        runtime_branch="{{ params.runtime_branch }}",
        microservice_path=MICROSERVICE_PATH,
        dag_branch="{{ params.dag_branch }}",
        dag_image_tag="{{ params.dag_image_tag }}",
        arguments=[_ARGS_SHELL],
        # deferrable=True,
        # poll_interval=60,
        **kpo_common,
    )

    # ── kubernetes + containerized ──────────────────────────────────────────
    # Image ENTRYPOINT is preserved. Pass each flag/value as a separate list
    # item so Kubernetes delivers them as individual args to the entrypoint.
    EasyKubernetesPodOperator(
        task_id="k8s_containerized",
        orchestration_mode="kubernetes",
        runtime_mode="containerized",
        runtime_image="etl/instagram",
        runtime_image_tag="{{ params.runtime_image_tag }}",
        dag_branch="{{ params.dag_branch }}",
        dag_image_tag="{{ params.dag_image_tag }}",
        arguments=_ARGS_LIST,
        # deferrable=True,
        # poll_interval=60,
        **kpo_common,
    )
