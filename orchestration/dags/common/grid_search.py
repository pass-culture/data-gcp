from itertools import product
import hashlib
import json

from typing import Callable
from enum import Enum
from airflow.models import DAG, Param
from airflow.operators.empty import EmptyOperator
from airflow.models import BaseOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from common.operators.gce import (
    StartGCEOperator,
    InstallDependenciesOperator,
    DeleteGCEOperator,
)


class InvalidGridError(Exception):
    """Raised when a grid DAG cannot generate any valid parameter combinations."""


class ExecutionMode(str, Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    DISPATCH = "dispatch"


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

    _DEFAULT_MAX_PARALLEL_VMS = 8  # internal constant, read-only

    def __init__(
        self,
        ml_task_fn: Callable[
            [dict[str, object], str, str], BaseOperator | list[BaseOperator]
        ],
        param_grid=None,
        common_params=None,
        search_mode=ParameterSearchMode.COMBINATORIAL,
        execution_mode=ExecutionMode.PARALLEL,
        n_vms=1,
        start_vm_kwargs=None,
        install_deps_kwargs=None,
        use_params_for_grid_config=False,
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
            search_mode (ParameterSearchMode): ORTHOGONAL or COMBINATORIAL
            execution_mode (ExecutionMode): PARALLEL, SEQUENTIAL, or DISPATCH mode.
            n_vms (int): Number of VMs to launch (used in DISPATCH mode).
            start_vm_kwargs (dict, optional): kwargs for StartGCEOperator.
            install_deps_kwargs (dict, optional): kwargs for InstallDependenciesOperator.
            use_params_for_grid_config (bool): If True, reads grid configuration from
                                               DAG params instead of constructor args.
            *args: Additional DAG args.
            **kwargs: Additional DAG kwargs.
        """
        # Add grid configuration params if not already present and use_params_for_grid_config is True
        if use_params_for_grid_config:
            default_params = {
                "param_grid_json": Param(
                    default=json.dumps(param_grid or {}),
                    type="string",
                    description="JSON string of parameter grid for grid search",
                ),
                "common_params_json": Param(
                    default=json.dumps(common_params or {}),
                    type="string",
                    description="JSON string of common parameters shared across all tasks",
                ),
                "search_mode": Param(
                    default=search_mode.value
                    if isinstance(search_mode, ParameterSearchMode)
                    else search_mode,
                    enum=[mode.value for mode in ParameterSearchMode],
                    description="Parameter search mode: combinatorial or orthogonal",
                ),
                "execution_mode": Param(
                    default=execution_mode.value
                    if isinstance(execution_mode, ExecutionMode)
                    else execution_mode,
                    enum=[mode.value for mode in ExecutionMode],
                    description="Execution mode: parallel, sequential, or dispatch",
                ),
                "n_vms": Param(
                    default=n_vms,
                    type="integer",
                    description="Number of VMs to launch",
                    minimum=1,
                ),
            }
            # Merge with existing params
            existing_params = kwargs.get("params", {})
            kwargs["params"] = {**default_params, **existing_params}

        super().__init__(*args, **kwargs)

        if not callable(ml_task_fn):
            raise ValueError("ml_task_fn must be a callable")

        self.ml_task_fn = ml_task_fn
        self.use_params_for_grid_config = use_params_for_grid_config

        # Store original values as defaults
        self._default_param_grid = param_grid or {}
        self._default_common_params = common_params or {}
        self._default_search_mode = search_mode
        self._default_execution_mode = execution_mode
        self._default_n_vms = n_vms

        # These will be set properly in _resolve_grid_config()
        self.param_grid = None
        self.common_params = None
        self.search_mode = None
        self.execution_mode = None
        self.n_vms = None

        self.start_vm_kwargs = start_vm_kwargs or {}
        self.install_deps_kwargs = install_deps_kwargs or {}
        self.tasks_chains = []
        self.max_parallel_vms = self._DEFAULT_MAX_PARALLEL_VMS

    # ----------------- PROTECTED PROPERTY -----------------

    @property
    def max_parallel_vms(self):
        """Read-only max number of parallel VMs to limit cost."""
        return self._max_parallel_vms

    @max_parallel_vms.setter
    def max_parallel_vms(self, value):
        if hasattr(self, "_max_parallel_vms"):
            raise AttributeError("max_parallel_vms is read-only and cannot be modified")
        self._max_parallel_vms = value

    # -----------------------------------------------------

    def _resolve_grid_config(self):
        """
        Resolve grid configuration from DAG params or use constructor defaults.

        This method must be called at parse time (in build_grid) to determine the
        actual configuration to use.

        Note: When use_params_for_grid_config is True, this reads the DEFAULT values
        from params (not runtime values), since DAG structure must be determined at
        parse time.
        """
        if self.use_params_for_grid_config and hasattr(self, "params"):
            try:
                # Get default values from params (parse-time)
                param_grid_param = self.params.get("param_grid_json")
                common_params_param = self.params.get("common_params_json")
                search_mode_param = self.params.get("search_mode")
                execution_mode_param = self.params.get("execution_mode")
                n_vms_param = self.params.get("n_vms")

                # Parse JSON strings
                if param_grid_param:
                    default_value = (
                        param_grid_param.value
                        if hasattr(param_grid_param, "value")
                        else param_grid_param
                    )
                    self.param_grid = (
                        json.loads(default_value)
                        if isinstance(default_value, str)
                        else default_value
                    )
                else:
                    self.param_grid = self._default_param_grid

                if common_params_param:
                    default_value = (
                        common_params_param.value
                        if hasattr(common_params_param, "value")
                        else common_params_param
                    )
                    self.common_params = (
                        json.loads(default_value)
                        if isinstance(default_value, str)
                        else default_value
                    )
                else:
                    self.common_params = self._default_common_params

                # Parse enums
                if search_mode_param:
                    default_value = (
                        search_mode_param.value
                        if hasattr(search_mode_param, "value")
                        else search_mode_param
                    )
                    self.search_mode = ParameterSearchMode(default_value)
                else:
                    self.search_mode = self._default_search_mode

                if execution_mode_param:
                    default_value = (
                        execution_mode_param.value
                        if hasattr(execution_mode_param, "value")
                        else execution_mode_param
                    )
                    self.execution_mode = ExecutionMode(default_value)
                else:
                    self.execution_mode = self._default_execution_mode

                if n_vms_param:
                    default_value = (
                        n_vms_param.value
                        if hasattr(n_vms_param, "value")
                        else n_vms_param
                    )
                    self.n_vms = int(default_value)
                else:
                    self.n_vms = self._default_n_vms

            except (json.JSONDecodeError, ValueError) as e:
                raise ValueError(f"Failed to parse grid configuration from params: {e}")
        else:
            # Use constructor defaults
            self.param_grid = self._default_param_grid
            self.common_params = self._default_common_params
            self.search_mode = self._default_search_mode
            self.execution_mode = self._default_execution_mode
            self.n_vms = self._default_n_vms

    # -----------------------------------------------------

    def generate_param_combinations(self) -> list[dict[str, object]]:
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
                merged = {**self.common_params, **comb}  # grid values override defaults
                combinations.append(merged)

        elif self.search_mode == ParameterSearchMode.ORTHOGONAL:
            for key, values in self.param_grid.items():
                default = self.common_params.get(key, values[0])
                for v in values:
                    if v == default:
                        continue
                    comb = self.common_params.copy()
                    comb[key] = v
                    combinations.append(comb)

        if not combinations:
            raise InvalidGridError(
                "No valid parameter combinations could be generated "
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
            - Resolves grid configuration from params or constructor defaults
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
        # Resolve configuration from params or defaults
        self._resolve_grid_config()

        combinations = self.generate_param_combinations()
        if not combinations:
            raise InvalidGridError(
                f"No valid parameter combinations to build DAG (param_grid={self.param_grid})"
            )

        if self.execution_mode == "SEQUENTIAL":
            self.n_vms = 1
        elif self.execution_mode == "DISPATCH":
            self.n_vms = min(len(combinations), self.max_parallel_vms)
        elif self.execution_mode == "PARALLEL":
            if len(combinations) > self.max_parallel_vms:
                self.execution_mode = "DISPATCH"
            self.n_vms = min(len(combinations), self.max_parallel_vms)

        start_grid = EmptyOperator(task_id="start_grid")
        end_grid = EmptyOperator(task_id="end_grid", trigger_rule=TriggerRule.ALL_DONE)

        instance_names = [f"{self.dag_id}_vm_{i}" for i in range(self.n_vms)]
        vm_to_tasks_chains: dict[int, list[tuple[int, dict[str, object]]]] = {
            vm_idx: [] for vm_idx in range(self.n_vms)
        }

        # Distribute task chain parameter combinations round-robin across VMs
        for comb_idx, params in enumerate(combinations):
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
