import datetime
import logging
from functools import partial

from common.callback import on_failure_base_callback
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from common.dbt.dag_utils import (
    dbt_dag_reconstruction,
    load_and_process_manifest,
)
from common.dbt.dbt_executors import (
    compile_dbt_with_selector,
    clean_dbt,
    run_dbt_model,
    run_dbt_test,
    run_dbt_snapshot,
)
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
)
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "start_date": datetime.datetime(2020, 12, 23),
    "retries": 6,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_base_callback,
}

# Load manifest and process it
(
    manifest,
    dbt_snapshots,
    dbt_models,
    dbt_crit_tests,
    models_with_dependencies,
    models_with_crit_test_dependencies,
    crit_test_parents,
) = load_and_process_manifest(f"{PATH_TO_DBT_TARGET}")

# Initialize the DAG
dag_id = "dbt_run_dag"
dag = DAG(
    dag_id,
    default_args=default_args,
    dagrun_timeout=datetime.timedelta(minutes=480),
    catchup=False,
    description="A dbt wrapper for airflow using Python operators",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[dag_id]),
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
        "GLOBAL_CLI_FLAGS": Param(
            default="--no-write-json",
            type="string",
        ),
        "full_refresh": Param(
            default=False,
            type="boolean",
        ),
    },
    tags=[DAG_TAGS.DBT.value, DAG_TAGS.DE.value],
)

# Define initial and final tasks
start = EmptyOperator(task_id="start", dag=dag, pool="dbt")
end = EmptyOperator(task_id="end", dag=dag, pool="dbt")

wait_for_raw = delayed_waiting_operator(
    dag=dag,
    external_dag_id="import_applicative_database",
    pool="dbt",
)

wait_for_firebase = delayed_waiting_operator(
    dag=dag,
    external_dag_id="import_intraday_firebase_data",
    allowed_states=["success", "upstream_failed"],
    failed_states=["failed"],
)

end_wait = EmptyOperator(
    task_id="end_wait", dag=dag, trigger_rule="none_failed_min_one_success", pool="dbt"
)

data_transfo_checkpoint = EmptyOperator(
    task_id="data_transfo_checkpoint", dag=dag, pool="dbt"
)

snapshots_checkpoint = EmptyOperator(
    task_id="snapshots_checkpoint", dag=dag, pool="dbt"
)

# Create Python operators
clean = PythonOperator(
    task_id="cleanup",
    python_callable=clean_dbt,
    pool="dbt",
    dag=dag,
    trigger_rule="none_failed_min_one_success",
)

compile = PythonOperator(
    task_id="compilation",
    python_callable=partial(
        compile_dbt_with_selector,
        selector="package:data_gcp_dbt",
        use_tmp_artifacts=False,
    ),
    dag=dag,
)

# Run the dbt DAG reconstruction
operator_dict = dbt_dag_reconstruction(
    dag,
    manifest,
    dbt_models,
    dbt_snapshots,
    models_with_crit_test_dependencies,
    crit_test_parents,
    clean,
)

##### DAG orchestration

# Set dependencies for model operations
model_tasks = list(operator_dict["model_op_dict"].values())
snapshot_tasks = list(operator_dict["snapshot_op_dict"].values())

# Defining the task dependency flow
(
    start
    >> [wait_for_raw, wait_for_firebase]
    >> end_wait
    >> data_transfo_checkpoint
    >> model_tasks
)
start >> operator_dict["trigger_block"]
end_wait >> snapshots_checkpoint >> snapshot_tasks
end_wait >> compile
compile >> (model_tasks + snapshot_tasks) >> clean >> end
