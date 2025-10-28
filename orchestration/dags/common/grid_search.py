from itertools import product
import hashlib
import re

from enum import Enum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from common.operators.gce import (
    StartGCEOperator,
    InstallDependenciesOperator,
    DeleteGCEOperator,
)


class ExecutionMode(str, Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    DISPATCH = "dispatch"


class ParameterSearchMode(str, Enum):
    COMBINATORIC = "combinatoric"
    ORTHOGONAL = "orthogonal"


class GridDAG(DAG):
    """
    DAG subclass for running parameter grid search workflows on Google Cloud VMs.

    Features:
        - Generates combinations of parameters either
            - all combinations of parameters (combinatorics)
            - orthogonal basis along each params, other axis are set to default
        - Launches a configurable number of VMs in parallel or sequentially.
        - Installs dependencies on each VM.
        - Chains ML tasks for each parameter combination with safe Airflow task IDs.
        - Supports hashing long parameter values to keep task IDs valid.
        - Optional remapping of parameter values for shorter task IDs.
    """

    def __init__(
        self,
        param_grid=None,
        common_params=None,
        search_mode=ParameterSearchMode.COMBINATORIC,
        execution_mode=ExecutionMode.PARALLEL,
        n_vms=1,
        start_vm_kwargs=None,
        install_deps_kwargs=None,
        max_suffix_len: int = 200,
        *args,
        **kwargs,
    ):
        """
        Initialize the GridDAG.

        Args:
            param_grid (dict, optional): Dictionary of parameter lists for grid search.
            common_params (dict, optional): Parameters shared across all tasks.
            search_mode (ParameterSearch_mode): ORTHOGONAL or COMBINATORIC
            execution_mode (ExecutionMode): PARALLEL, SEQUENTIAL, or DISPATCH mode.
            n_vms (int): Number of VMs to launch (used in DISPATCH mode).
            start_vm_kwargs (dict, optional): kwargs for StartGCEOperator.
            install_deps_kwargs (dict, optional): kwargs for InstallDependenciesOperator.
            max_suffix_len (int): Maximum allowed length of generated task suffixes.
            *args: Additional DAG args.
            **kwargs: Additional DAG kwargs.
        """
        super().__init__(*args, **kwargs)
        self.param_grid = param_grid or {}
        self.common_params = common_params or {}
        self.execution_mode = execution_mode
        self.search_mode = search_mode
        self.n_vms = n_vms
        self.start_vm_kwargs = start_vm_kwargs or {}
        self.install_deps_kwargs = install_deps_kwargs or {}
        self.chains = []
        self.max_suffix_len = max_suffix_len

        # Optional remap attribute, can be set externally if needed
        self.remap: dict[str, dict[str, str]] | None = None

    def generate_param_combinations(self):
        """
        Generate parameter combinations based on search_mode.

        Supported modes:
            - **COMBINATORIC**:
                Generates the full Cartesian product of all parameters in `param_grid`.
                Example:
                    param_grid = {"lr": [0.01, 0.1], "batch": [32, 64]}
                    → 4 combinations:
                        [{'lr':0.01,'batch':32}, {'lr':0.01,'batch':64},
                        {'lr':0.1,'batch':32}, {'lr':0.1,'batch':64}]
            - **ORTHOGONAL**:
                Generates one combination per parameter dimension, varying one parameter
                at a time while keeping all other parameters fixed to their default
                values from `common_params`.
                Example:
                    param_grid = {"lr": [0.01, 0.1], "batch": [32, 64]}
                    common_params = {"lr": 0.01, "batch": 32}
                    → 2 combinations:
                        [{'lr':0.1,'batch':32}, {'lr':0.01,'batch':64}]

        Returns:
            List[Tuple[dict, str | None]]: Each tuple contains:
                - param_dict: dictionary of parameters for this combination
                - varied_key: the key that is actually varied in this combination
                (None in COMBINATORIC mode)

        Notes:
            - If `param_grid` is empty, returns a single combination with `common_params`.
            - If all grid values equal their defaults in ORTHOGONAL mode, a single
            default combination is returned.
        """
        # If no grid parameters, return a single default combination
        if not self.param_grid:
            return [(self.common_params.copy(), None)]

        combinations = []

        if self.search_mode == ParameterSearchMode.COMBINATORIC:
            # Standard Cartesian product
            keys = list(self.param_grid.keys())
            for values in product(*self.param_grid.values()):
                comb = dict(zip(keys, values))
                comb.update(self.common_params)  # merge defaults
                combinations.append((comb, None))  # None because all keys matter

        elif self.search_mode == ParameterSearchMode.ORTHOGONAL:
            # One parameter varied at a time, others fixed to defaults
            for key, values in self.param_grid.items():
                for v in values:
                    comb = self.common_params.copy()
                    comb[key] = v  # set the varied value
                    combinations.append((comb, key))  # mark which key was varied

            # If no variations differ from defaults, include the default combination
            if not combinations:
                combinations.append((self.common_params.copy(), None))

        return combinations

    def _generate_suffix(
        self, params: dict, idx: int, varied_key: str | None = None
    ) -> str:
        """
        Generate a unique suffix using only grid parameters (from param_grid keys).
        Hashes long values automatically, prefixes hash with the param name.

        Optionally, a remap dictionary can provide shorter representations.

        Args:
            params (dict): Parameter combination dictionary.
            idx (int): Index of the combination for uniqueness.
            varied_key (str | None): Key that is actually varied (ORTHOGONAL mode),
                                        or None (COMBINATORIC mode).

        Returns:
            str: Safe, unique task suffix.
        """
        parts = []

        # Determine which keys to include in the suffix
        if self.search_mode == ParameterSearchMode.ORTHOGONAL:
            if varied_key:
                # Include the varied key plus any other keys that differ from default
                keys_to_use = [
                    k
                    for k in self.param_grid.keys()
                    if k == varied_key or params.get(k) != self.common_params.get(k)
                ]
            else:
                # Default combination: include all grid keys that differ from default
                keys_to_use = [
                    k
                    for k in self.param_grid.keys()
                    if params.get(k) != self.common_params.get(k)
                ]
        else:
            keys_to_use = self.param_grid.keys()

        for k in keys_to_use:
            if k not in params:
                continue
            v_str = str(params[k])

            # Apply remap dictionary if defined
            if self.remap and k in self.remap:
                v_str = self.remap[k].get(v_str, v_str)
            # Hash long values to avoid overly long suffixes
            elif len(v_str) > 50:
                v_str = hashlib.md5(v_str.encode()).hexdigest()[:8]

            # Sanitize: keep only valid characters for task_id
            v_str = re.sub(r"[^a-zA-Z0-9._-]", "_", v_str)
            parts.append(f"{k}_{v_str}")

        # Construct final suffix
        suffix = f"{idx}_{'_'.join(parts)}" if parts else str(idx)

        # Fallback: truncate further if still too long
        if len(suffix) > self.max_suffix_len:
            hash_part = hashlib.md5("_".join(parts).encode()).hexdigest()[:8]
            suffix = f"{idx}_{hash_part}"

        return suffix

    def build_grid(self, ml_task_fn):
        """
        Construct the DAG structure for running a parameter grid search workflow.

        This method:
            - Generates all parameter combinations from `param_grid` and merges with `common_params`.
            - Determines the number of VMs to launch based on `execution_mode`.
            - Creates start and stop VM tasks, plus dependency installation tasks per VM.
            - Assigns each parameter combination to a VM (round-robin) and generates
            unique, valid task suffixes for each combination.
            - Calls `ml_task_fn` for each combination to produce the ML tasks and chains them sequentially.
            - Ensures proper task ordering and safe cleanup of VMs.
            - Returns DAG boundary markers for integration with other tasks.

        Args:
            ml_task_fn (Callable[[dict, str, str], Union[BaseOperator, list[BaseOperator]]]):
                A function that accepts a parameter dictionary, VM instance name,
                and unique suffix, and returns one or more Airflow tasks representing
                the workflow for that parameter combination.

        Returns:
            Tuple[EmptyOperator, EmptyOperator]:
                A tuple containing the start and end DAG markers (`EmptyOperator` tasks)
                that enclose the grid workflow. These can be used to chain upstream
                or downstream tasks in the DAG.

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
        combinations = self.generate_param_combinations()
        if not combinations:
            return

        # Determine number of VMs
        if self.execution_mode == ExecutionMode.SEQUENTIAL:
            self.n_vms = 1
        elif self.execution_mode == ExecutionMode.PARALLEL:
            self.n_vms = len(combinations)
        # DISPATCH -> use specified n_vms

        start_grid = EmptyOperator(task_id="start_grid")
        end_grid = EmptyOperator(task_id="end_grid", trigger_rule=TriggerRule.ALL_DONE)

        instance_names = [f"{self.dag_id}_vm_{i}" for i in range(self.n_vms)]
        vm_to_chains = {i: [] for i in range(self.n_vms)}
        for idx, (params, varied_key) in enumerate(combinations):
            vm_idx = idx % self.n_vms
            vm_to_chains[vm_idx].append((idx, params, varied_key))

        for vm_idx, chains in vm_to_chains.items():
            instance_name = instance_names[vm_idx]

            start_vm = StartGCEOperator(
                task_id=f"start_vm_{vm_idx}",
                instance_name=instance_name,
                **self.start_vm_kwargs,
            )
            start_grid >> start_vm

            install = InstallDependenciesOperator(
                task_id=f"install_{vm_idx}",
                instance_name=instance_name,
                **self.install_deps_kwargs,
            )
            start_vm >> install

            prev_task = install
            for idx, params, varied_key in chains:
                # Generate suffix using the varied_key (or all keys for combinatoric)
                suffix_task = self._generate_suffix(params, idx, varied_key)
                ml_task_sequence = ml_task_fn(params, instance_name, suffix_task)

                if not isinstance(ml_task_sequence, list):
                    ml_task_sequence = [ml_task_sequence]

                for i, task in enumerate(ml_task_sequence):
                    if prev_task != install and (i % len(ml_task_sequence)) == 0:
                        task.trigger_rule = TriggerRule.ALL_DONE
                    prev_task >> task
                    prev_task = task

            stop_vm = DeleteGCEOperator(
                task_id=f"stop_vm_{vm_idx}",
                instance_name=instance_name,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )
            prev_task >> stop_vm
            stop_vm >> end_grid

        return start_grid, end_grid
