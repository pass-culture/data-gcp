from itertools import product
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


def generate_unique_suffix(params: dict, idx: int, max_len: int = 200) -> str:
    """
    Generate a unique suffix for task IDs based on parameter combinations.

    Raises:
        ValueError: if the generated suffix exceeds max_len characters.
    """
    param_part = "_".join(f"{k}_{str(v).replace('.', '_')}" for k, v in params.items())
    suffix = f"{idx}_{param_part}" if param_part else str(idx)

    if len(suffix) > max_len:
        raise ValueError(
            f"Generated suffix too long ({len(suffix)} chars). "
            f"Reduce parameter name/value lengths or number of grid parameters.\n"
            f"Suffix: {suffix}"
        )

    return suffix


class GridDAG(DAG):
    def __init__(
        self,
        param_grid=None,
        common_params=None,
        execution_mode=ExecutionMode.PARALLEL,
        n_vms=1,
        start_vm_kwargs=None,
        install_deps_kwargs=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.param_grid = param_grid or {}
        self.common_params = common_params or {}
        self.execution_mode = execution_mode
        self.n_vms = n_vms
        self.start_vm_kwargs = start_vm_kwargs or {}
        self.install_deps_kwargs = install_deps_kwargs or {}
        self.chains = []

    def generate_param_combinations(self):
        keys = self.param_grid.keys()
        values_product = list(product(*self.param_grid.values()))
        combinations = [dict(zip(keys, values)) for values in values_product]

        # merge common params into each combination
        for comb in combinations:
            comb.update(self.common_params)
        return combinations

    def build_grid(self, ml_task_fn):
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

        # assign chains to VMs (round-robin)
        instance_names = [f"{self.dag_id}-vm-{i}" for i in range(self.n_vms)]
        vm_to_chains = {i: [] for i in range(self.n_vms)}
        for idx, params in enumerate(combinations):
            vm_idx = idx % self.n_vms  # assign to VM in round-robin
            vm_to_chains[vm_idx].append((idx, params))

        # create one Start/Stop VM wrapper per VM
        for vm_idx, chains in vm_to_chains.items():
            instance_name = instance_names[vm_idx]

            # Start VM
            start_vm = StartGCEOperator(
                task_id=f"start_vm_{vm_idx}",
                instance_name=instance_name,
                **self.start_vm_kwargs,
            )
            start_grid >> start_vm

            # Install dependencies
            install = InstallDependenciesOperator(
                task_id=f"install_{vm_idx}",
                instance_name=instance_name,
                **self.install_deps_kwargs,
            )
            start_vm >> install

            # Sequential ML tasks inside VM wrapper
            prev_task = install
            for idx, params in chains:
                suffix_task = generate_unique_suffix(params, idx)
                ml_task_sequence = ml_task_fn(params, instance_name, suffix_task)

                if not isinstance(ml_task_sequence, list):
                    ml_task_sequence = [ml_task_sequence]

                # Chain the tasks sequentially
                for i, task in enumerate(ml_task_sequence):
                    # trigger the next chain even if previous failed
                    if prev_task != install and (i % len(ml_task_sequence)) == 0:
                        task.trigger_rule = TriggerRule.ALL_DONE
                    prev_task >> task
                    prev_task = task

            # Delete VM after all tasks
            stop_vm = DeleteGCEOperator(
                task_id=f"stop_vm_{vm_idx}",
                instance_name=instance_name,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # guarantee deletion with initial empty operator
            )
            prev_task >> stop_vm
            stop_vm >> end_grid

        return start_grid, end_grid
