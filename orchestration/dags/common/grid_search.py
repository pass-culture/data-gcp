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


class GridDAG(DAG):
    """
    DAG subclass for running parameter grid search workflows on Google Cloud VMs.

    Features:
        - Generates all combinations of grid parameters.
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
        self.n_vms = n_vms
        self.start_vm_kwargs = start_vm_kwargs or {}
        self.install_deps_kwargs = install_deps_kwargs or {}
        self.chains = []
        self.max_suffix_len = max_suffix_len

        # Optional remap attribute, can be set externally if needed
        self.remap: dict[str, dict[str, str]] | None = None

    def generate_param_combinations(self):
        """
        Generate all combinations of grid parameters merged with common parameters.

        Returns:
            List[dict]: A list of dictionaries, each representing a combination of parameters.
        """
        keys = self.param_grid.keys()
        values_product = list(product(*self.param_grid.values()))
        combinations = [dict(zip(keys, values)) for values in values_product]

        # merge common params into each combination
        for comb in combinations:
            comb.update(self.common_params)
        return combinations

    def _generate_suffix(self, params: dict, idx: int) -> str:
        """
        Generate a unique suffix using only grid parameters (from param_grid keys).
        Hashes long values automatically, prefixes hash with the param name.

        Optionally, a remap dictionary can provide shorter representations.

        Args:
            params (dict): Parameter combination dictionary.
            idx (int): Index of the combination for uniqueness.

        Returns:
            str: Safe, unique task suffix.
        """
        parts = []
        grid_keys = self.param_grid.keys()  # pick directly from DAG attributes

        for k in grid_keys:
            if k not in params:
                continue
            v = params[k]
            v_str = str(v)

            # Use remap if available
            if self.remap and k in self.remap:
                v_str = self.remap[k].get(v_str, v_str)
            # Hash long values (>50 chars)
            elif len(v_str) > 50:
                hash_part = hashlib.md5(v_str.encode()).hexdigest()[:8]
                v_str = f"{hash_part}"

            # sanitize: keep only valid chars
            v_str = re.sub(r"[^a-zA-Z0-9._-]", "_", v_str)
            parts.append(f"{k}_{v_str}")

        suffix = f"{idx}_{'_'.join(parts)}" if parts else str(idx)

        # fallback truncate if still too long
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

        # determine number of VMs
        if self.execution_mode == ExecutionMode.SEQUENTIAL:
            self.n_vms = 1
        elif self.execution_mode == ExecutionMode.PARALLEL:
            self.n_vms = len(combinations)
        # DISPATCH -> use specified n_vms

        start_grid = EmptyOperator(task_id="start_grid")
        end_grid = EmptyOperator(task_id="end_grid", trigger_rule=TriggerRule.ALL_DONE)

        instance_names = [f"{self.dag_id}-vm-{i}" for i in range(self.n_vms)]
        vm_to_chains = {i: [] for i in range(self.n_vms)}
        for idx, params in enumerate(combinations):
            vm_idx = idx % self.n_vms
            vm_to_chains[vm_idx].append((idx, params))

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
            for idx, params in chains:
                suffix_task = self._generate_suffix(params, idx)
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
