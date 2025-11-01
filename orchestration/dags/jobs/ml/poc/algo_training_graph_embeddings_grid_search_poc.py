"""
# Airflow DAG: Graph Embeddings Grid Search (POC)

This is a POC:
- can be used as a template for other grid search upon adapting the ml_task_chain function.
— future iterations may include UI-controlled grid search and dynamic configuration.

Notes:
- To prevent Drag&Drop dags such as this one to be deleted during a MEP hence interupting their running tasks.
    Place this dag in a dedicated subfolder containing a .rsyncignore file to automatically exclude that folder and its contents from deployment.
    Make sure there are no orchestrated dags in the same folder.
- Scheduling is forcefully disabled as it's purpose is manual runs.
"""

import hashlib
import json
from datetime import datetime, timedelta
from enum import Enum
from itertools import product
from typing import Callable

from airflow import DAG
from airflow.models import BaseOperator, Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Common Airflow imports (internal project modules)
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    INSTANCES_TYPES,
    ML_BUCKET_TEMP,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)

# =============================================================================
# Helper Functions & Custom Exceptions
# =============================================================================


def _normalize_instance_name(name: str) -> str:
    """Normalize GCE instance names by replacing underscores with dashes."""
    return name.replace("_", "-")


def format_poc_dag_doc(
    dag_name: str, search_mode: str, n_vms: int, grid_params: dict, shared_params: dict
) -> str:
    """
    Returns a nicely formatted POC DAG documentation string.
    """
    if search_mode == "points":
        grid_params_str = "\n".join(
            [f"{json.dumps(point, indent=2)}\n" for point in grid_params]
        )
    else:
        grid_params_str = json.dumps(grid_params, indent=2)
    shared_params_str = json.dumps(shared_params, indent=2)

    doc = f"""
    # Airflow DAG: {dag_name} (POC)

    ## ⚠️ NOTICE – Proof of Concept (POC)

    This is a POC — future iterations may include UI-controlled grid search and dynamic configuration.

    ### Current Limitations:
    - Search parameters **cannot be modified dynamically via Airflow UI**.
    - All grid configurations must be **hardcoded in the DAG file**.

    ### Recommended Usage:
    1. Create a **dedicated dev branch** with your desired parameter search configuration.
    2. Coordinate with your team to avoid parallel modifications or DAG name collisions.
    3. Upload (`drag & drop`) this DAG file into your Airflow DAGs bucket.
    4. Trigger the DAG manually in the Airflow UI.

    Notes:
    - To prevent Drag&Drop dags such as this one to be deleted during a MEP hence interupting their running tasks.
        Place this dag in a dedicated subfolder containing a .rsyncignore file to automatically exclude that folder and its contents from deployment.
        Make sure there are no orchestrated dags in the same folder.
    - Scheduling is forcefully disabled as it's purpose is manual runs.

    ---

    ## Overview
    This DAG runs a **parameter grid search** for graph embedding model training on Google Cloud VMs.
    It supports both **combinatorial** and **orthogonal** parameter search modes via the `GridDAG` abstraction.

    The DAG launches up to 10 VM(s), installs dependencies, trains the embedding model,
    evaluates results, and stores metrics and embeddings to GCS.

    ## Current Setup:

    ### VMs number: {n_vms}

    ### Search mode: {search_mode}

    ### Grid Search {"Parameters" if search_mode != "points" else "Points"}
    \n{grid_params_str}

    ### Shared Parameters
    \n{shared_params_str}

    """
    return doc


class InvalidGridError(Exception):
    """Raised when no valid parameter combinations can be generated for the grid search."""


class ParameterSearchMode(str, Enum):
    """Defines supported parameter search strategies."""

    COMBINATORIAL = "combinatorial"
    ORTHOGONAL = "orthogonal"
    POINTS = "points"


# =============================================================================
# GridDAG Class — Core Abstraction for Parameterized VM-Based Workflows
# =============================================================================


class GridDAG(DAG):
    """
    Specialized DAG subclass for running parameterized ML experiments on Google Cloud VMs.
    Scheduling is forcefully disabled as it's purpose is manual runs.

    Supports:
        - Full combinatorial parameter search (Cartesian product).
        - Orthogonal parameter search (vary one param at a time).
        - Automated VM lifecycle: start → install dependencies → run ML → delete.
        - Hash-safe task suffixes to prevent Airflow task ID collisions.

    Example Usage:
        >>> dag = GridDAG(
        >>>     dag_id="my_grid_dag",
        >>>     ml_task_fn=my_ml_task_chain,
        >>>     grid_params={"lr": [0.001, 0.01], "batch": [32, 64]},
        >>>     common_params={"optimizer": "adam"},
        >>>     search_mode=ParameterSearchMode.COMBINATORIAL,
        >>> )

    Notes:
        - The `ml_task_fn` defines the actual ML workflow to run for each combination.
        - The only supported workflows are linear task sequences.
        - Each VM runs multiple parameter configurations (round-robin distributed).
    """

    _DEFAULT_MAX_PARALLEL_VMS = 10

    def __init__(
        self,
        ml_task_fn: Callable[
            [dict[str, object], str, str], BaseOperator | list[BaseOperator]
        ],
        grid_params=None,
        common_params=None,
        search_mode=ParameterSearchMode.COMBINATORIAL,
        n_vms=1,
        start_vm_kwargs=None,
        install_deps_kwargs=None,
        *args,
        **kwargs,
    ):
        # Scheduling disabled for drag & drop dags
        if "schedule_interval" in kwargs:
            del kwargs["schedule_interval"]
        kwargs["schedule_interval"] = None

        super().__init__(*args, **kwargs)
        if not callable(ml_task_fn):
            raise ValueError("`ml_task_fn` must be a callable returning Airflow tasks.")

        self.ml_task_fn = ml_task_fn
        self.grid_params = grid_params or {}
        self.common_params = common_params or {}
        self.search_mode = search_mode
        self.start_vm_kwargs = start_vm_kwargs or {}
        self.install_deps_kwargs = install_deps_kwargs or {}

        # Compute parameter combinations
        self.params_combinations = self._generate_params_combinations()

        # Limit number of VMs for safety
        self.n_vms = min(
            n_vms, self._DEFAULT_MAX_PARALLEL_VMS, len(self.params_combinations or [])
        )

    # -------------------------------------------------------------------------
    # Parameter Combination Logic
    # -------------------------------------------------------------------------
    def _generate_params_combinations(self) -> list[dict[str, object]]:
        """
        Generate parameter combinations based on the selected search mode.

        Supported modes:
            - COMBINATORIAL → Full Cartesian product of parameters.
            - ORTHOGONAL → One combination per parameter dimension.

        Returns:
            list[dict[str, object]]: Parameter combinations merged with common params.

        Raises:
            InvalidGridError: If no valid combinations can be generated.
        """
        if not self.grid_params:
            return [self.common_params.copy()]

        combinations = []

        if self.search_mode == ParameterSearchMode.COMBINATORIAL:
            keys = list(self.grid_params.keys())
            for values in product(*self.grid_params.values()):
                merged = {**self.common_params, **dict(zip(keys, values))}
                combinations.append(merged)

        elif self.search_mode == ParameterSearchMode.ORTHOGONAL:
            for key, values in self.grid_params.items():
                for v in values:
                    comb = self.common_params.copy()
                    comb[key] = v
                    combinations.append(comb)

        elif self.search_mode == ParameterSearchMode.POINTS:
            # Expect grid_params to be a list of dicts
            if not isinstance(self.grid_params, list):
                raise InvalidGridError(
                    "For POINTS mode, `grid_params` must be a list of parameter dictionaries."
                )

            for conf in self.grid_params:
                if not isinstance(conf, dict):
                    raise InvalidGridError(
                        f"Invalid config in POINTS mode: expected dict, got {type(conf)}"
                    )
                merged = {**self.common_params, **conf}
                combinations.append(merged)

        else:
            raise InvalidGridError(f"Unsupported search mode: {self.search_mode}")

        if not combinations:
            raise InvalidGridError(f"Invalid grid configuration: {self.grid_params}")

        return combinations

    # -------------------------------------------------------------------------
    # Task ID & Suffix Handling
    # -------------------------------------------------------------------------
    def _generate_suffix(self, params: dict[str, object], idx: int) -> str:
        """
        Generate unique suffix for task IDs based on parameter content.

        Example:
            {'lr':0.01,'batch':32} → "0_5f1d3b9c2a6e7d8f"
        """
        combined_str = "_".join(
            f"{k}={json.dumps(v, sort_keys=True)}" for k, v in sorted(params.items())
        )
        hashed = hashlib.md5(combined_str.encode()).hexdigest()[:16]
        return f"{idx}_{hashed}"

    # -------------------------------------------------------------------------
    # Build the DAG workflow
    # -------------------------------------------------------------------------
    def build_grid(self) -> tuple[EmptyOperator, EmptyOperator]:
        """
        Construct the parameter grid workflow:
            - Spawns N VMs
            - Runs each parameter combination on a VM (round-robin)
            - Installs dependencies → runs ML tasks → cleans up VM

        Returns:
            (EmptyOperator, EmptyOperator): Start and end boundary tasks for chaining.
        """
        if not self.params_combinations:
            raise InvalidGridError("No valid parameter combinations found.")

        start_grid = EmptyOperator(task_id="start_grid")
        end_grid = EmptyOperator(task_id="end_grid", trigger_rule=TriggerRule.ALL_DONE)

        # # Distribute parameter combos across VMs (round-robin)
        vm_to_tasks_chains = {i: [] for i in range(self.n_vms)}
        for comb_idx, params in enumerate(self.params_combinations):
            vm_idx = comb_idx % self.n_vms
            vm_to_tasks_chains[vm_idx].append((comb_idx, params))

        with TaskGroup(group_id="grid_group") as grid_group:
            for vm_idx, task_chains in vm_to_tasks_chains.items():
                # Merge template instance name with VM index for uniqueness
                instance_name_template = self.start_vm_kwargs.get(
                    "instance_name", self.dag_id
                )
                instance_name = _normalize_instance_name(
                    f"{instance_name_template}-vm-{vm_idx}"
                )

                start_vm = StartGCEOperator(
                    task_id=f"start_vm_{vm_idx}",
                    instance_name=instance_name,
                    **self.start_vm_kwargs,
                )

                install = InstallDependenciesOperator(
                    task_id=f"install_{vm_idx}",
                    instance_name=instance_name,
                    **self.install_deps_kwargs,
                )
                start_vm >> install

                prev_task = install
                for comb_idx, params in task_chains:
                    suffix = self._generate_suffix(params, comb_idx)
                    ml_tasks = self.ml_task_fn(params, instance_name, suffix)
                    ml_tasks = (
                        [ml_tasks] if not isinstance(ml_tasks, list) else ml_tasks
                    )

                    for task_idx, task in enumerate(ml_tasks):
                        if prev_task != install and task_idx % len(ml_tasks) == 0:
                            task.trigger_rule = TriggerRule.ALL_DONE
                        prev_task >> task
                        prev_task = task

                stop_vm = DeleteGCEOperator(
                    task_id=f"stop_vm_{vm_idx}",
                    instance_name=instance_name,
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                prev_task >> stop_vm

        start_grid >> grid_group >> end_grid
        return start_grid, end_grid


# =============================================================================
# ML Task Chain Definition
# =============================================================================


def ml_task_chain(params, instance_name, suffix):
    """
    Defines the ML tasks to run for each parameter combination.

    Each chain typically includes:
        1. Training the model
        2. Evaluating embeddings
        3. Saving results to GCS

    Args:
        params (dict): Parameter set for this run (merged grid + common params).
        instance_name (str): Assigned GCE instance.
        suffix (str): Unique hash suffix for task IDs and output paths.

    Returns:
        list[BaseOperator]: Sequential Airflow tasks. `build_grid` handles connecting to VM lifecycle tasks.
    """
    config_json = json.dumps(params)

    train = SSHGCEOperator(
        task_id=f"train_{suffix}",
        instance_name=instance_name,
        base_dir=BASE_DIR,
        command=(
            "cli train-metapath2vec "
            "{{ params.experiment_name }} "
            f"{STORAGE_BASE_PATH}/raw_input/ "
            f"--output-embeddings {STORAGE_BASE_PATH}/{suffix}_{EMBEDDINGS_FILENAME} "
            "{% if params['train_only_on_10k_rows'] %} --nrows 10000 {% endif %} "
            f"--config '{config_json}'"
        ),
        deferrable=True,
    )

    evaluate = SSHGCEOperator(
        task_id=f"evaluate_{suffix}",
        instance_name=instance_name,
        base_dir=BASE_DIR,
        command=(
            "cli evaluate-metapath2vec "
            f"{STORAGE_BASE_PATH}/raw_input/ "
            f"{STORAGE_BASE_PATH}/{suffix}_{EMBEDDINGS_FILENAME} "
            f"{STORAGE_BASE_PATH}/{suffix}_evaluation_metrics.csv "
            f"--output-scores-path {STORAGE_BASE_PATH}/{suffix}_evaluation_scores_details.parquet"
        ),
        deferrable=True,
    )

    return [train, evaluate]


# =============================================================================
# DAG Configuration & Initialization
# =============================================================================

# DAG Metadata
DATE = "{{ ts_nodash }}"
DAG_NAME = "graph_embeddings_grid"
DEFAULT_ARGS = {
    "start_date": datetime(2023, 5, 9),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# GCE Defaults
INSTANCE_NAME = f"grid-train-graph-embeddings-{ENV_SHORT_NAME}"
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-16",
    "prod": "n1-standard-16",
}[ENV_SHORT_NAME]

# Paths and Tables
GCS_FOLDER_PATH = f"algo_training_{ENV_SHORT_NAME}/{DAG_NAME}_{DATE}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
BASE_DIR = "data-gcp/jobs/ml_jobs/graph_recommendation"
EMBEDDINGS_FILENAME = "embeddings.parquet"
INPUT_TABLE_NAME = "item_with_metadata_to_embed"

# Grid Search Parameters
N_VMS = 4
SEARCH_MODE = ParameterSearchMode.POINTS
GRID_PARAMS: dict[dict | list[dict]] = {
    "combinatorial": {"embedding_dim": [32, 64, 128], "context_size": [5, 10, 15]},
    "orthogonal": {"embedding_dim": [32, 64, 128], "context_size": [5, 10, 15]},
    "points": [
        {"embedding_dim": 64, "batch_size": 200},
        {
            "embedding_dim": 128,
            "num_negative_samples": 5,
            "walks_per_node": 5,
            "batch_size": 100,
        },
        {"embedding_dim": 256, "batch_size": 50},
        {"num_negative_samples": 20, "batch_size": 25},
        {"num_negative_samples": 10, "batch_size": 50},
        {"walks_per_node": 20, "batch_size": 25},
        {"walks_per_node": 10, "batch_size": 50},
    ],
}
SHARED_PARAMS = {}

DOC_MD = format_poc_dag_doc(
    DAG_NAME, SEARCH_MODE.value, N_VMS, GRID_PARAMS[SEARCH_MODE.value], SHARED_PARAMS
)
# =============================================================================
# DAG Definition
# =============================================================================

with GridDAG(
    dag_id=DAG_NAME,
    description="Grid search training for graph embeddings (POC)",
    ml_task_fn=ml_task_chain,
    grid_params=GRID_PARAMS[SEARCH_MODE.value],
    common_params=SHARED_PARAMS,
    search_mode=SEARCH_MODE,
    n_vms=N_VMS,
    start_date=datetime(2023, 1, 1),
    dagrun_timeout=timedelta(minutes=1200),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    render_template_as_native_obj=True,
    tags=[
        DAG_TAGS.POC.value,
        DAG_TAGS.DS.value,
        DAG_TAGS.VM.value,
        DAG_TAGS.DRAGNDROP.value,
    ],
    doc_md=DOC_MD,
    start_vm_kwargs={
        "preemptible": False,
        "instance_type": "{{ params.instance_type }}",
        "gpu_type": "{{ params.gpu_type }}",
        "gpu_count": "{{ params.gpu_count }}",
        "labels": {"job_type": "long_ml"},
    },
    install_deps_kwargs={
        "python_version": "3.12",
        "base_dir": BASE_DIR,
        "branch": "{{ params.branch }}",
        "retries": 2,
    },
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=INSTANCE_TYPE, enum=INSTANCES_TYPES["cpu"]["standard"]
        ),
        "instance_name": Param(default=INSTANCE_NAME, type="string"),
        "gpu_type": Param(
            default="nvidia-tesla-t4", enum=INSTANCES_TYPES["gpu"]["name"]
        ),
        "gpu_count": Param(default=1, enum=INSTANCES_TYPES["gpu"]["count"]),
        "experiment_name": Param(
            default="algo_training_graph_embeddings_v1_grid", type="string"
        ),
        "train_only_on_10k_rows": Param(default=True, type="boolean"),
    },
) as dag:
    # Initial extraction step: prepare training data from BigQuery
    start = EmptyOperator(task_id="start")

    import_offer_as_parquet = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="import_offer_as_parquet",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_GRAPH_RECOMMENDATION_DATASET,
                    "tableId": INPUT_TABLE_NAME,
                },
                "compression": None,
                "destinationUris": f"{STORAGE_BASE_PATH}/raw_input/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
    )

    # Build and link the grid workflow
    start_grid, end_grid = dag.build_grid()

    start >> import_offer_as_parquet >> start_grid >> end_grid
