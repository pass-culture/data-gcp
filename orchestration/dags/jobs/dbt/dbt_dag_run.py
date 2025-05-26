import datetime

from common.callback import on_failure_base_callback
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from common.dbt.utils import dbt_dag_reconstruction, load_and_process_manifest
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
)
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "start_date": datetime.datetime(2020, 12, 23),
    "retries": 6,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_base_callback,
    "on_skipped_callback": on_failure_base_callback,
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
    description="A dbt wrapper for airflow",
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
start = DummyOperator(task_id="start", dag=dag, pool="dbt")
end = DummyOperator(task_id="end", dag=dag, trigger_rule="none_failed", pool="dbt")

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
end_wait = DummyOperator(
    task_id="end_wait", dag=dag, trigger_rule="none_failed", pool="dbt"
)
data_transfo_checkpoint = DummyOperator(
    task_id="data_transfo_checkpoint", dag=dag, pool="dbt"
)
snapshots_checkpoint = DummyOperator(
    task_id="snapshots_checkpoint", dag=dag, pool="dbt"
)


clean = BashOperator(
    task_id="cleanup",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_clean_tmp_folders.sh ",
    env={
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
    },
    pool="dbt",
    dag=dag,
)


compile = BashOperator(
    task_id="compilation",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_compile.sh ",
    env={
        "target": "{{ params.target }}",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
    pool="dbt",
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
    >> compile
    >> [wait_for_raw, wait_for_firebase]
    >> end_wait
    >> data_transfo_checkpoint
    >> model_tasks
)
start >> operator_dict["trigger_block"]
end_wait >> snapshots_checkpoint >> snapshot_tasks
(model_tasks + snapshot_tasks) >> clean >> end
