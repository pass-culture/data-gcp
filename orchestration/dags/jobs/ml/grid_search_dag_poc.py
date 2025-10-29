import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.models import BaseOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
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
    SSHGCEOperator,
    StartGCEOperator,
    InstallDependenciesOperator,
    DeleteGCEOperator,
)

from itertools import product
import hashlib

from typing import Callable
from enum import Enum


def _normalize_instance_name(name: str) -> str:
    return name.replace("_", "-")


class InvalidGridError(Exception):
    """Raised when a grid DAG cannot generate any valid parameter combinations."""


class ParameterSearchMode(str, Enum):
    COMBINATORIAL = "combinatorial"
    ORTHOGONAL = "orthogonal"


class GridDAG(DAG):
    """
    DAG subclass for running parameter grid search workflows on Google Cloud VMs.

    Features:
        - Generates combinations of parameters either
            - all combinations of parameters (combinatorial)
            - orthogonal basis along each params, other axis are set to default
        - Launches a configurable number of VMs in parallel or sequentially.
        - Installs dependencies on each VM.
        - tasks_chains ML tasks for each parameter combination with safe Airflow task IDs.
        - Supports hashing long parameter values to keep task IDs valid.
        - Optional remapping of parameter values for shorter task IDs.
    """

    _DEFAULT_MAX_PARALLEL_VMS = 10  # Internal Safeguard, read-only

    def __init__(
        self,
        ml_task_fn: Callable[
            [dict[str, object], str, str], BaseOperator | list[BaseOperator]
        ],
        param_grid=None,
        common_params=None,
        search_mode=ParameterSearchMode.COMBINATORIAL,
        n_vms=1,
        start_vm_kwargs=None,
        install_deps_kwargs=None,
        *args,
        **kwargs,
    ):
        """
        Initialize the GridDAG.

        Args:
            ml_task_fn: Function accepting (params, vm_name, suffix) returning
                        BaseOperator or list of BaseOperators.
            param_grid (dict, optional): Dictionary of parameter lists for grid search.
            common_params (dict, optional): Parameters shared across all tasks.
            search_mode (ParameterSearch_mode): ORTHOGONAL or COMBINATORIAL
            execution_mode (ExecutionMode): PARALLEL, SEQUENTIAL, or DISPATCH mode.
            n_vms (int): Number of VMs to launch (used in DISPATCH mode).
            start_vm_kwargs (dict, optional): kwargs for StartGCEOperator.
            install_deps_kwargs (dict, optional): kwargs for InstallDependenciesOperator.
            *args: Additional DAG args.
            **kwargs: Additional DAG kwargs.
        """
        super().__init__(*args, **kwargs)
        if not callable(ml_task_fn):
            raise ValueError("ml_task_fn must be a callable")

        self.ml_task_fn = ml_task_fn
        self.param_grid = param_grid or {}
        self.common_params = common_params or {}
        self.search_mode = search_mode
        self.start_vm_kwargs = start_vm_kwargs or {}
        self.install_deps_kwargs = install_deps_kwargs or {}
        self.tasks_chains = []
        self.params_combinations = self._generate_params_combinations()
        self.n_vms = min(
            n_vms, self._DEFAULT_MAX_PARALLEL_VMS, len(self.params_combinations or [])
        )

    def _generate_params_combinations(self) -> list[dict[str, object]]:
        """
        Generate all parameter combinations based on search_mode.

        Supported modes:
            - COMBINATORIAL:
                Generates the full Cartesian product of all parameters in `param_grid`.
                Example:
                    param_grid = {"lr": [0.01, 0.1], "batch": [32, 64]}
                    common_params = {"optimizer": "adam"}
                    → 4 combinations:
                        [
                            {'lr':0.01,'batch':32,'optimizer':'adam'},
                            {'lr':0.01,'batch':64,'optimizer':'adam'},
                            {'lr':0.1,'batch':32,'optimizer':'adam'},
                            {'lr':0.1,'batch':64,'optimizer':'adam'}
                        ]
            - ORTHOGONAL:
                Generates one combination per parameter dimension, varying one parameter
                at a time while keeping other parameters fixed to defaults.
                Example:
                    param_grid = {"lr": [0.01, 0.1], "batch": [32, 64]}
                    common_params = {"lr": 0.01, "batch": 32, "optimizer": "adam"}
                    → 2 combinations:
                        [
                            {'lr':0.1,'batch':32,'optimizer':'adam'},
                            {'lr':0.01,'batch':64,'optimizer':'adam'}
                        ]

        Returns:
            List[Dict[str, object]]: Each dict represents a parameter combination.

        Raises:
            InvalidGridError: If no valid combinations can be generated.
        """
        if not self.param_grid:
            return [self.common_params.copy()]

        combinations: list[dict[str, object]] = []

        if self.search_mode == ParameterSearchMode.COMBINATORIAL:
            keys = list(self.param_grid.keys())
            for values in product(*self.param_grid.values()):
                comb = dict(zip(keys, values))
                merged = {**self.common_params, **comb}
                combinations.append(merged)

        elif self.search_mode == ParameterSearchMode.ORTHOGONAL:
            for key, values in self.param_grid.items():
                for v in values:
                    comb = self.common_params.copy()
                    comb[key] = v
                    combinations.append(comb)

        if not combinations:
            raise InvalidGridError(
                f"No valid parameter combinations could be generated "
                f"(search_mode={self.search_mode}, param_grid={self.param_grid}, "
                f"common_params={self.common_params})"
            )

        return combinations

    def _generate_suffix(self, params: dict[str, object], idx: int) -> str:
        """
        Generate a unique task suffix using all parameters. Hashes the string if too long.

        Args:
            params: Parameter combination dictionary
            idx: Index for uniqueness

        Returns:
            str: Safe, unique task suffix

        Example:
            params = {'lr':0.01,'batch':32,'optimizer':'adam'}
            idx = 0
            → "0_5f1d3b9c2a6e7d8f"  # hashed version of all params
        """
        combined_str = "_".join(f"{k}={v}" for k, v in sorted(params.items()))
        hashed = hashlib.md5(combined_str.encode()).hexdigest()[:16]

        return f"{idx}_{hashed}"

    def build_grid(self) -> tuple[EmptyOperator, EmptyOperator]:
        """
        Construct the DAG structure for running a parameter grid search workflow.

        This method:
            - Generates all parameter combinations from `param_grid` and merges with `common_params`.
            - Determines the number of VMs to launch based on `execution_mode`.
            - Creates start and stop VM tasks, plus dependency installation tasks per VM.
            - Assigns each parameter combination to a VM (round-robin) and generates
            unique, valid task suffixes for each combination.
            - Calls `ml_task_fn` for each combination to produce the ML tasks and tasks_chains them sequentially.
            - Ensures proper task ordering and safe cleanup of VMs.
            - Returns DAG boundary markers for integration with other tasks.

        Returns:
            Tuple[EmptyOperator, EmptyOperator]:
                A tuple containing the start and end DAG markers (`EmptyOperator` tasks)
                that enclose the grid workflow. These can be used to tasks_chain upstream
                or downstream tasks in the DAG.

        Raises:
            ValueError: If `ml_task_fn` was not provided when initializing the DAG.

        Guidelines for `ml_task_fn`:
            1. Must return either a single Airflow task or a list of tasks.
            2. Tasks returned should have unique task IDs using the provided suffix.
            3. Tasks will be executed sequentially on the assigned VM.
            4. Avoid returning `None` or using `chain()` as the return value.
            5. Task IDs must only contain valid Airflow characters:
            alphanumeric, underscore (_), dash (-), or period (.).
            6. Use the `suffix` for filenames, logging, or tagging outputs to keep
            each parameter combination traceable.
        """

        if not self.params_combinations:
            raise InvalidGridError(
                f"No valid parameter combinations to build DAG (param_grid={self.param_grid})"
            )

        start_grid = EmptyOperator(task_id="start_grid")
        end_grid = EmptyOperator(task_id="end_grid", trigger_rule=TriggerRule.ALL_DONE)

        instance_names = [
            _normalize_instance_name(f"{self.dag_id}-vm-{i}") for i in range(self.n_vms)
        ]
        vm_to_tasks_chains: dict[int, list[tuple[int, dict[str, object]]]] = {
            vm_idx: [] for vm_idx in range(self.n_vms)
        }

        # Distribute task chain parameter combinations round-robin across VMs
        for comb_idx, params in enumerate(self.params_combinations):
            vm_idx = comb_idx % self.n_vms
            vm_to_tasks_chains[vm_idx].append((comb_idx, params))

        with TaskGroup(group_id="grid_group") as grid_group:
            for vm_idx, tasks_chains in vm_to_tasks_chains.items():
                instance_name = instance_names[vm_idx]

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
                for comb_idx, params in tasks_chains:
                    # Generate hashed suffix for uniqueness
                    suffix = self._generate_suffix(params, comb_idx)
                    ml_tasks = self.ml_task_fn(params, instance_name, suffix)
                    if not isinstance(ml_tasks, list):
                        ml_tasks = [ml_tasks]

                    for task_idx, task in enumerate(ml_tasks):
                        if prev_task != install and task_idx % len(ml_tasks) == 0:
                            task.trigger_rule = TriggerRule.ALL_DONE
                        prev_task >> task
                        prev_task = task

                stop_vm = DeleteGCEOperator(
                    task_id=f"stop_vm_{vm_idx}",
                    instance_name=instance_name,
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )
                prev_task >> stop_vm

        start_grid >> grid_group >> end_grid
        return start_grid, end_grid


# DAG metadata
DATE = "{{ ts_nodash }}"
DAG_NAME = "algo_training_graph_embeddings_grid"
DEFAULT_ARGS = {
    "start_date": datetime(2023, 5, 9),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# GCE defaults
INSTANCE_NAME = f"grid-train-graph-embeddings-{ENV_SHORT_NAME}"
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-16",
    "prod": "n1-standard-16",
}[ENV_SHORT_NAME]

# Paths
GCS_FOLDER_PATH = f"algo_training_{ENV_SHORT_NAME}/{DAG_NAME}_{DATE}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
BASE_DIR = "data-gcp/jobs/ml_jobs/graph_recommendation"
EMBEDDINGS_FILENAME = "embeddings.parquet"

# BigQuery tables
INPUT_TABLE_NAME = "item_with_metadata_to_embed"
EMBEDDING_TABLE_NAME = "graph_embedding"


# ML task chain
def ml_task_chain(params, instance_name, suffix):
    """
    Create Airflow tasks for a single parameter combination in a GridDAG.

    Args:
        params (dict): Parameter values for this combination; merged from the grid and
                        any common parameters by `build_grid`.
        instance_name (str): GCE instance where tasks run; assigned by `build_grid`.
        suffix (str): Unique identifier for task IDs and output files, generated by
                        `build_grid` to avoid collisions across combinations.

    Returns:
        List[BaseOperator]: Ordered tasks representing this combination’s workflow.
                            Internal structure (linear, parallel, or branched) is
                            preserved; `build_grid` handles connecting to VM lifecycle tasks.
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
        do_xcom_push=True,
    )

    evaluate = SSHGCEOperator(
        task_id=f"evaluate_{suffix}",
        instance_name=instance_name,
        base_dir=BASE_DIR,
        command=(
            "cli evaluate-metapath2vec "
            f"{{{{ ti.xcom_pull(task_ids='train_{suffix}') }}}} "
            f"{STORAGE_BASE_PATH}/raw_input/ "
            f"{STORAGE_BASE_PATH}/{suffix}_{EMBEDDINGS_FILENAME} "
            f"{STORAGE_BASE_PATH}/{suffix}_evaluation_metrics.csv "
            f"--output-scores-path {STORAGE_BASE_PATH}/{suffix}_evaluation_scores_details.parquet"
        ),
        deferrable=True,
    )

    return [train, evaluate]


# Grid Search parameters
SEARCH_MODE = ParameterSearchMode.COMBINATORIAL
N_VMS = 4
PARAM_GRID = {"embedding_dim": [32, 64, 128], "context_size": [5, 10, 15]}
SHARED_PARAMS = {
    "base_dir": BASE_DIR,
}


with GridDAG(
    dag_id="algo_training_graph_embeddings_grid_search",
    description="Grid search training for graph embeddings",
    ml_task_fn=ml_task_chain,
    param_grid=PARAM_GRID,
    common_params=SHARED_PARAMS,
    search_mode=SEARCH_MODE,
    n_vms=N_VMS,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=1200),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    render_template_as_native_obj=True,
    tags=[DAG_TAGS.POC.value, DAG_TAGS.DS.value, DAG_TAGS.VM.value],
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
            default="algo_training_graph_embeddings_v1", type="string"
        ),
        "train_only_on_10k_rows": Param(default=True, type="boolean"),
    },
) as dag:
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

    start_grid, end_grid = dag.build_grid()

    start >> import_offer_as_parquet >> start_grid >> end_grid
