import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from airflow.models import Param
from airflow.operators.python import BranchPythonOperator
from common.alerts import task_fail_slack_alert
from common.utils import get_airflow_schedule, waiting_operator
from common.dbt.utils import load_manifest
from common import macros

from common.config import (
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    ENV_SHORT_NAME,
    PATH_TO_DBT_TARGET,
    EXCLUDED_TAGS,
)


default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    "dbt_run_dag",
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


# branching function for skipping waiting task when dag is triggered manually
def choose_branch(**context):
    run_id = context["dag_run"].run_id
    if run_id.startswith("scheduled__"):
        return ["wait_for_dbt_init_dag_end"]
    return ["manual_trigger_shunt"]


start = DummyOperator(task_id="start", dag=dag)

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=choose_branch,
    provide_context=True,
    dag=dag,
)
shunt = DummyOperator(task_id="manual_trigger_shunt", dag=dag)

wait4init = waiting_operator(dag, "dbt_init_dag")

join = DummyOperator(task_id="join", dag=dag, trigger_rule="none_failed")

end = DummyOperator(task_id="end", dag=dag, trigger_rule="none_failed")

wait_for_raw = waiting_operator(dag=dag, dag_id="import_applicative_database")

# Dbt dag reconstruction
model_op_dict = {}
test_op_dict = {}


manifest = load_manifest(f"{PATH_TO_DBT_TARGET}")

dbt_models = [
    node
    for node in manifest["nodes"].keys()
    if (
        manifest["nodes"][node]["resource_type"] == "model"
        and manifest["nodes"][node]["package_name"] == "data_gcp_dbt"
    )
]
dbt_crit_tests = [
    node
    for node in manifest["nodes"].keys()
    if (
        manifest["nodes"].get(node).get("resource_type") == "test"
        and manifest["nodes"].get(node).get("package_name") == "data_gcp_dbt"
    )
    and manifest["nodes"][node]["config"].get("severity", "warn").lower() == "error"
]
models_with_dependencies = [
    node for node in manifest["child_map"].keys() if node in dbt_models
]
models_with_crit_test_dependencies = [
    manifest["nodes"][node].get("attached_node") for node in dbt_crit_tests
]
crit_test_parents = {
    manifest["nodes"][test].get("attached_node", None): [
        parent
        for parent in set(manifest["parent_map"][test]).intersection(set(dbt_models))
    ]
    for test in dbt_crit_tests
}
models_with_dependencies = [
    node
    for node in manifest["child_map"].keys()
    if (node in dbt_models)
    and (
        manifest["nodes"][node]["resource_type"] == "model"
        and manifest["nodes"][node]["package_name"] == "data_gcp_dbt"
    )
]

# first create test operators and hide them in a group
with TaskGroup(group_id="critical_tests", dag=dag) as crit_test_group:
    for model_node in dbt_models:
        full_ref_str = " --full-refresh" if not "{{ params.full_refresh }}" else ""
        model_data = manifest["nodes"][model_node]
        if model_node in models_with_crit_test_dependencies:
            test_op_dict[model_node] = BashOperator(
                task_id=model_data["alias"] + "_tests",
                bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test_model.sh ",
                env={
                    "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                    "target": "{{ params.target }}",
                    "model": f"""{model_data['alias']}""",
                    "full_ref_str": full_ref_str,
                    "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
                dag=dag,
            )

# loop over models
with TaskGroup(group_id="data_transformation", dag=dag) as data_transfo:
    with TaskGroup(group_id="applicative_tables", dag=dag) as applicative:
        for model_node in dbt_models:
            # hide numerous applicative tables
            if "applicative" in manifest["nodes"][model_node]["alias"]:
                full_ref_str = (
                    " --full-refresh"
                    if "{{ params.full_refresh|lower }}" == "true"
                    else ""
                )
                model_data = manifest["nodes"][model_node]
                # with TaskGroup(group_id=f"{model_data['alias']}_tasks", dag=dag):
                model_op_dict[model_node] = BashOperator(
                    task_id=model_data["alias"],
                    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run.sh ",
                    env={
                        "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                        "target": "{{ params.target }}",
                        "model": f"{model_data['name']}",
                        "full_ref_str": full_ref_str,
                        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                        "EXCLUSION": " --exclude "
                        + " ".join([f"tag:{item}" for item in EXCLUDED_TAGS])
                        if len(EXCLUDED_TAGS) > 0
                        else "",
                    },
                    append_env=True,
                    cwd=PATH_TO_DBT_PROJECT,
                    dag=dag,
                )
                # create dependencies between tests and their attached model
                if model_node in models_with_crit_test_dependencies:
                    model_op_dict[model_node] >> test_op_dict[model_node]

    for model_node in dbt_models:
        if "applicative" not in manifest["nodes"][model_node]["alias"]:
            full_ref_str = " --full-refresh" if not "{{ params.full_refresh }}" else ""
            model_data = manifest["nodes"][model_node]
            # with TaskGroup(group_id=f"{model_data['alias']}_tasks", dag=dag):
            model_op_dict[model_node] = BashOperator(
                task_id=model_data["name"],
                bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run.sh ",
                env={
                    "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                    "target": "{{ params.target }}",
                    "model": f"{model_data['name']}",
                    "full_ref_str": full_ref_str,
                    "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                    "EXCLUSION": " --exclude "
                    + " ".join([f"tag:{item}" for item in EXCLUDED_TAGS])
                    if len(EXCLUDED_TAGS) > 0
                    else "",
                },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
                dag=dag,
            )
            # create dependencies between tests and their attached model
            if model_node in models_with_crit_test_dependencies:
                model_op_dict[model_node] >> test_op_dict[model_node]

    # set up models ascendencies
    for model_node in dbt_models:
        full_ref_str = " --full-refresh" if not "{{ params.full_refresh }}" else ""
        model_data = manifest["nodes"][model_node]
        children = tuple(
            [
                model_op_dict[child]
                for child in manifest["child_map"][model_node]
                if child in dbt_models
            ]
        )
        # replace model ascendency by test ascendency when needed
        if model_node in models_with_crit_test_dependencies:
            test_op_dict[model_node] >> (children if len(children) > 0 else end)
        else:
            model_op_dict[model_node] >> (children if len(children) > 0 else end)


# test's cross dependencies management
for test, parents in crit_test_parents.items():
    for p in parents:
        try:
            model_op_dict[p] >> test_op_dict[test]
        except KeyError:
            pass

start >> branching >> [shunt, wait4init] >> join >> wait_for_raw >> data_transfo
