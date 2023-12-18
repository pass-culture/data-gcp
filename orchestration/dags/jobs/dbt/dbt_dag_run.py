import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from airflow.models import Param
from common.alerts import dbt_test_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)
from common.dbt.utils import rebuild_manifest, load_run_results

from common import macros
from common.config import (
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    ENV_SHORT_NAME,
    PATH_TO_DBT_TARGET,
)


default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "dbt_run_dag",
    default_args=default_args,
    catchup=False,
    description="A dbt wrapper for airflow",
    schedule_interval=get_airflow_schedule("0 3 * * *"),
    params={
        "target": Param(
            default=ENV_SHORT_NAME,
            type="string",
        ),
        "GLOBAL_CLI_FLAGS": Param(
            default="",
            type="string",
        ),
        "full_refresh": Param(
            default=False,
            type="boolean",
        ),
    },
)

# Basic steps
start = DummyOperator(task_id="start", dag=dag)

dbt_dep_op = BashOperator(
    task_id="dbt_deps",
    bash_command="dbt deps --target {{ params.target }}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

dbt_compile_op = BashOperator(
    task_id="dbt_compile",
    bash_command="dbt compile --target {{ params.target }} "
    + f"--target-path {PATH_TO_DBT_TARGET}",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)


# Dbt dag reconstruction
model_op_dict = {}
test_op_dict = {}

simplified_manifest = rebuild_manifest(PATH_TO_DBT_TARGET)

with TaskGroup(group_id="data_transformation", dag=dag) as data_transfo:
    full_ref_str = " --full-refresh" if not "{{ params.full_refresh }}" else ""
    # models task group
    for model_node, model_data in simplified_manifest.items():
        crit_tests_list = model_data["model_tests"].get("error", [])
        crit_tests_list = [
            test
            for test in crit_tests_list
            if not test["test_alias"].endswith(f'ref_{model_data["model_alias"]}_')
        ]
        with TaskGroup(
            group_id=f'{model_data["model_alias"]}_tasks', dag=dag
        ) as model_tasks:
            # models
            model_op = BashOperator(
                task_id=model_data["model_alias"],
                bash_command=f"bash {PATH_TO_DBT_PROJECT}/dbt_run.sh ",
                env={
                    "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                    "target": "{{ params.target }}",
                    "model": f"{model_data['model_alias']}",
                    "full_ref_str": full_ref_str,
                    "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
                dag=dag,
            )
            model_op_dict[model_data["model_alias"]] = model_op
            # critical tests task subgroup
            if len(crit_tests_list) > 0:
                with TaskGroup(
                    group_id=f'{model_data["model_alias"]}_critical_tests', dag=dag
                ) as crit_tests_task:
                    dbt_test_tasks = [
                        BashOperator(
                            task_id=test["test_alias"],
                            bash_command=f"bash {PATH_TO_DBT_PROJECT}/dbt_run.sh "
                            if test["test_type"] == "generic"
                            else f"bash {PATH_TO_DBT_PROJECT}/dbt_test.sh ",
                            env={
                                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                                "target": "{{ params.target }}",
                                "model": f"{model_data['model_alias']}",
                                "full_ref_str": full_ref_str,
                                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                            },
                            append_env=True,
                            cwd=PATH_TO_DBT_PROJECT,
                            dag=dag,
                        )
                        for test in crit_tests_list
                    ]
                    if len(dbt_test_tasks) > 0:
                        model_op >> crit_tests_task

                for i, test in enumerate(crit_tests_list):
                    if not test["test_alias"].endswith(
                        f'ref_{model_data["model_alias"]}_'
                    ):
                        if test["test_alias"] not in test_op_dict.keys():
                            test_op_dict[test["test_alias"]] = {
                                "parent_model": [model_data["model_alias"]],
                                "test_op": dbt_test_tasks[i],
                            }
                        else:
                            test_op_dict[test["test_alias"]]["parent_model"] += [
                                model_data["model_alias"]
                            ]
            simplified_manifest[model_node]["redirect_dep"] = model_tasks

# data quality test group
with TaskGroup(group_id="data_quality_testing", dag=dag) as data_quality:
    full_ref_str = " --full-refresh" if not "{{ params.full_refresh }}" else ""
    for model_node, model_data in simplified_manifest.items():
        quality_tests_list = model_data["model_tests"].get("warn", [])
        quality_tests_list = [
            test
            for test in quality_tests_list
            if not test["test_alias"].endswith(f'ref_{model_data["model_alias"]}_')
        ]
        if len(quality_tests_list) > 0:
            # model testing subgroup
            with TaskGroup(
                group_id=f'{model_data["model_alias"]}_quality_tests', dag=dag
            ) as quality_tests_task:
                dbt_test_tasks = [
                    BashOperator(
                        task_id=test["test_alias"],
                        bash_command=f"bash {PATH_TO_DBT_PROJECT}/dbt_run.sh "
                        if test["test_type"] == "generic"
                        else f"bash {PATH_TO_DBT_PROJECT}/dbt_test.sh ",
                        env={
                            "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                            "target": "{{ params.target }}",
                            "model": f"{model_data['model_alias']}",
                            "full_ref_str": full_ref_str,
                            "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                        },
                        append_env=True,
                        cwd=PATH_TO_DBT_PROJECT,
                        dag=dag,
                    )
                    for test in quality_tests_list
                ]
                for i, test in enumerate(quality_tests_list):
                    if test["test_alias"] not in test_op_dict.keys():
                        test_op_dict[test["test_alias"]] = {
                            "parent_model": [model_data["model_alias"]],
                            "test_op": dbt_test_tasks[i],
                        }
                    else:
                        test_op_dict[test["test_alias"]]["parent_model"] += [
                            model_data["model_alias"]
                        ]
            if len(dbt_test_tasks) > 0:
                model_op_dict[model_data["model_alias"]] >> quality_tests_task
    # models' task groups dependencies
    for node in simplified_manifest.keys():
        for upstream_node in simplified_manifest[node]["depends_on_node"]:
            if upstream_node is not None:
                if upstream_node.startswith("model."):
                    try:
                        (
                            simplified_manifest[upstream_node]["redirect_dep"]
                            >> simplified_manifest[node]["redirect_dep"]
                        )
                    except:
                        pass
                else:
                    pass

# tests' cross dependencies management
for test_alias, details in test_op_dict.items():
    for parent in details["parent_model"]:
        model_op_dict[parent] >> details["test_op"]


# TO DO : gather test warnings logs and send them to slack alert task through xcom
# alerting_task = DummyOperator(task_id="dummy_quality_alerting_task", dag=dag)


with TaskGroup(group_id="alerting_tasks", dag=dag) as alert_tasks:
    run_results = load_run_results(PATH_TO_DBT_TARGET)
    failed_crit = {}
    failed_qual = {}
    for p_node, p_node_attributes in simplified_manifest.items():
        crit_tests = p_node_attributes["model_tests"].get("error", [])
        qual_tests = p_node_attributes["model_tests"].get("warn", [])
        if len(crit_tests) > 0:
            # print(f"crit test: {crit_tests}")
            out = {
                p_node: [
                    {
                        test[
                            "test_node"
                        ]: f"status: {run_results[test['test_node']]['status']} | message: {run_results[test['test_node']]['message']} | failures: {run_results[test['test_node']]['failures']}"
                    }
                    for test in crit_tests
                    if (run_results[test["test_node"]]["status"] != "pass")
                ]
            }
            failed_crit = (
                {**failed_crit, **out} if len(out[p_node]) else {**failed_crit, **{}}
            )
        if len(qual_tests) > 0:
            # print(f"qual test: {qual_tests}")
            out = {
                p_node: [
                    {
                        test[
                            "test_node"
                        ]: f"status: {run_results[test['test_node']]['status']} | message: {run_results[test['test_node']]['message']} | failures: {run_results[test['test_node']]['failures']}"
                    }
                    for test in qual_tests
                    if (run_results[test["test_node"]]["status"] != "pass")
                ]
            }
            failed_qual = (
                {**failed_qual, **out} if len(out[p_node]) else {**failed_qual, **{}}
            )

    # dag.add_task(dbt_test_fail_slack_alert(failed_crit,'critical'))
    # dag.add_task(dbt_test_fail_slack_alert(failed_qual,'quality'))
    crit_alerting_task = dbt_test_fail_slack_alert(failed_crit, "critical", dag)
    qual_alerting_task = dbt_test_fail_slack_alert(failed_qual, "quality", dag)

tests_op = BashOperator(
    task_id="dbt_tests",
    bash_command=f"""dbt test --target {{ params.target }} --no-compile""",
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)
# macrolevel dependencies
start >> dbt_dep_op >> dbt_compile_op >> data_transfo
(data_quality, data_transfo) >> tests_op >> alert_tasks >> end
