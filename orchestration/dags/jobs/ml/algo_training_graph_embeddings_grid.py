"""
Example: Using GridDAG with Airflow Params for Grid Configuration

This example shows how to make grid search configuration parameters
selectable in the Airflow UI using the params system.
"""

import json
from datetime import datetime, timedelta

from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from common.grid_search import GridDAG, ExecutionMode, ParameterSearchMode
from common.operators.gce import SSHGCEOperator

# ============================================================================
# OPTION 1: Using use_params_for_grid_config=True
# This makes grid configuration parameters editable in the Airflow UI
# ============================================================================

# Define your default parameter grid
DEFAULT_PARAM_GRID = {
    "embedding_dim": [32, 64, 128],
    "learning_rate": [0.001, 0.01],
}

DEFAULT_COMMON_PARAMS = {
    "base_dir": "data-gcp/jobs/ml_jobs/graph_recommendation",
}


def ml_task_chain(params, instance_name, suffix):
    """ML task chain function."""
    config_json = json.dumps(params)

    train = SSHGCEOperator(
        task_id=f"train_{suffix}",
        instance_name=instance_name,
        base_dir=params.get("base_dir", ""),
        command=f"cli train --config '{config_json}'",
        deferrable=True,
    )

    evaluate = SSHGCEOperator(
        task_id=f"evaluate_{suffix}",
        instance_name=instance_name,
        base_dir=params.get("base_dir", ""),
        command=f"cli evaluate --config '{config_json}'",
        deferrable=True,
    )

    return [train, evaluate]


# Create GridDAG with param-based configuration
with GridDAG(
    dag_id="ml_grid_search_with_params",
    description="Grid search with configurable parameters",
    ml_task_fn=ml_task_chain,
    # Default values (used at parse time)
    param_grid=DEFAULT_PARAM_GRID,
    common_params=DEFAULT_COMMON_PARAMS,
    search_mode=ParameterSearchMode.COMBINATORIAL,
    execution_mode=ExecutionMode.PARALLEL,
    n_vms=4,
    # Enable param-based configuration
    use_params_for_grid_config=True,
    # Standard DAG parameters
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=12),
    catchup=False,
    # VM configuration
    start_vm_kwargs={
        "preemptible": False,
        "instance_type": "{{ params.instance_type }}",
        "labels": {"job_type": "ml_training"},
    },
    install_deps_kwargs={
        "python_version": "3.12",
        "base_dir": "{{ params.base_dir }}",
        "retries": 2,
    },
    # Additional runtime params (these are runtime-configurable)
    params={
        "instance_type": Param(
            default="n1-standard-16",
            type="string",
            description="GCE instance type for ML training",
        ),
        "experiment_name": Param(
            default="grid_search_v1",
            type="string",
            description="Name of the experiment",
        ),
        # Grid configuration params are automatically added when use_params_for_grid_config=True
        # They will appear as:
        # - param_grid_json: JSON string of parameter grid
        # - common_params_json: JSON string of common parameters
        # - search_mode: Enum selector (combinatorial/orthogonal)
        # - execution_mode: Enum selector (parallel/sequential/dispatch)
        # - n_vms: Integer input for number of VMs
    },
) as dag:
    start = EmptyOperator(task_id="start")

    # Build the grid (this will use params or defaults)
    start_grid, end_grid = dag.build_grid()

    end = EmptyOperator(task_id="end")

    # Connect tasks
    start >> start_grid >> end_grid >> end


# # ============================================================================
# # OPTION 2: Traditional approach (no param-based configuration)
# # Grid configuration is fixed at DAG definition time
# # ============================================================================

# with GridDAG(
#     dag_id="ml_grid_search_traditional",
#     description="Grid search with fixed configuration",
#     ml_task_fn=ml_task_chain,

#     # Fixed configuration
#     param_grid=DEFAULT_PARAM_GRID,
#     common_params=DEFAULT_COMMON_PARAMS,
#     search_mode=ParameterSearchMode.ORTHOGONAL,
#     execution_mode=ExecutionMode.SEQUENTIAL,
#     n_vms=2,

#     # Do NOT enable param-based configuration
#     use_params_for_grid_config=False,  # This is the default

#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,

#     params={
#         "instance_type": Param(default="n1-standard-8", type="string"),
#         "experiment_name": Param(default="fixed_grid_v1", type="string"),
#     },
# ) as dag_traditional:
#     start_grid, end_grid = dag_traditional.build_grid()


# ============================================================================
# NOTES ON USAGE
# ============================================================================

"""
When use_params_for_grid_config=True:

1. BENEFITS:
   - Grid configuration appears in Airflow UI as editable params
   - Easy to modify without changing code
   - Values are validated (JSON for grids, enums for modes)
   
2. LIMITATIONS:
   - Configuration is still evaluated at DAG PARSE TIME, not runtime
   - Changing param values requires triggering a DAG re-parse
   - The DAG structure (number of tasks) is determined at parse time
   
3. HOW TO CHANGE CONFIGURATION:
   - Option A: Modify the default values in the params dict, then re-parse
   - Option B: Use Airflow CLI to update DAG params and trigger re-parse
   - Option C: Modify the Python file directly (traditional approach)

4. PARAM VISIBILITY:
   When you open the DAG in Airflow UI and trigger it, you'll see:
   
   - param_grid_json: Text input with JSON string
     Default: {"embedding_dim": [32, 64, 128], "learning_rate": [0.001, 0.01]}
   
   - common_params_json: Text input with JSON string
     Default: {"base_dir": "...", "optimizer": "adam"}
   
   - search_mode: Dropdown selector
     Options: "combinatorial", "orthogonal"
     Default: "combinatorial"
   
   - execution_mode: Dropdown selector
     Options: "parallel", "sequential", "dispatch"
     Default: "parallel"
   
   - n_vms: Number input
     Default: 4
     Minimum: 1

5. EXAMPLE JSON INPUTS:
   
   For param_grid_json:
   {
     "embedding_dim": [32, 64, 128],
     "learning_rate": [0.001, 0.01, 0.1],
     "batch_size": [16, 32]
   }
   
   For common_params_json:
   {
     "base_dir": "data-gcp/jobs/ml_jobs/graph_recommendation",
     "optimizer": "adam",
     "epochs": 100,
     "random_seed": 42
   }

6. SAFETY CONSIDERATIONS:
   - JSON validation happens at parse time
   - Invalid JSON will cause DAG parsing to fail (with helpful error message)
   - Enum values are validated against allowed options
   - n_vms is validated as integer with minimum value of 1
"""
