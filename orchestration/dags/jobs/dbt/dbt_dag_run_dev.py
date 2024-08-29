from common.alerts import task_fail_slack_alert
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from common.dbt.utils_dev import (
    create_folder_manual_trigger_group,
    dbt_dag_reconstruction,
    load_and_process_manifest,
)
from common.utils import get_airflow_schedule, waiting_operator

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime, timedelta

default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": task_fail_slack_alert,
}

# Initialize the DAG
dag = DAG(
    "dbt_run_dag_dev",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=240),
    catchup=False,
    description="A dbt wrapper for airflow",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
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
)

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

# Define initial and final tasks
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag, trigger_rule="none_failed")

wait_for_raw = waiting_operator(dag=dag, dag_id="import_applicative_database")
wait_for_firebase = waiting_operator(
    dag=dag,
    dag_id="import_intraday_firebase_data",
    external_task_id="end",
    allowed_states=["success", "upstream_failed"],
    failed_states=["failed"],
)
end_wait = DummyOperator(task_id="end_wait", dag=dag, trigger_rule="none_failed")
data_transfo_checkpoint = DummyOperator(task_id="data_transfo_checkpoint", dag=dag)
snapshots_checkpoint = DummyOperator(task_id="snapshots_checkpoint", dag=dag)

compile_op = BashOperator(
    task_id="compilation",
    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_compile.sh ",
    env={
        "target": "{{ params.target }}",
        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
    },
    append_env=True,
    cwd=PATH_TO_DBT_PROJECT,
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
    compile_op,
)
# Create the folder manual trigger TaskGroup
trigger_group = create_folder_manual_trigger_group(
    dag,
    dbt_models,
    manifest,
    operator_dict["model_op_dict"],
    group_name="folders_manual_trigger",
)

# Orchestrate the DAG using the returned TaskGroups and operators
(
    start
    >> [wait_for_raw, wait_for_firebase]
    >> end_wait
    >> data_transfo_checkpoint
    >> operator_dict["data_transfo_group"]
)
start >> trigger_group
end_wait >> snapshots_checkpoint >> operator_dict["snapshot_group"]
(
    (operator_dict["data_transfo_group"], operator_dict["snapshot_group"])
    >> compile_op
    >> end
)
