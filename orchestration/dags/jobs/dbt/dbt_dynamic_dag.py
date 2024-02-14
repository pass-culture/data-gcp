import datetime
import json

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from airflow.models import Param
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)
from common.dbt.utils import rebuild_manifest

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





with DAG(
    "dbt_dynamic_dag",
    default_args=default_args,
    catchup=False,
    description="A dbt wrapper for airflow",
    schedule_interval=None,#get_airflow_schedule("0 3 * * *"),
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
    ) as dag:

    start = DummyOperator(task_id="start")

    @task
    def buid_simplified_manifest(_PATH_TO_DBT_TARGET):
        simplified_manifest = rebuild_manifest(_PATH_TO_DBT_TARGET)
        return simplified_manifest
        # model_op_dict = {}
        # test_op_dict = {}
        # for model_node, model_data in simplified_manifest.items():
        #      crit_tests_list = model_data["model_tests"].get("error", [])
        #      model_op_dict[model_data["model_alias"]] = {'name': model_op_dict[model_data["model_alias"]]}
        #      if len(crit_tests_list) > 0:
        #           test_list = []
        #           for test in crit_tests_list:   
        #             if not test["test_alias"].endswith(f'ref_{model_data["model_alias"]}_'):
        #                 test_list.append(model_data['model_alias'])
    
    

    @task
    def make_model_task(simplified_manifest):
        model_op_dict = {}
        test_op_dict = {}
        full_ref_str = " --full-refresh" if "{{ params.full_refresh|lower }}" == 'true' else ""
         # models task group
        for model_node, model_data in simplified_manifest.items():
            crit_tests_list = model_data["model_tests"].get("error", [])
            with TaskGroup(group_id=f'{model_data["model_alias"]}_tasks') as model_tasks:
                # models
                model_op = BashOperator(
                    task_id=model_data["model_alias"],
                    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run.sh ",
                    env={
                        "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                        "target": "{{ params.target }}",
                        "model": f"{model_data['model_alias']}",
                        "full_ref_str": full_ref_str,
                        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                    },
                    append_env=True,
                    cwd=PATH_TO_DBT_PROJECT,
                )                
                model_op_dict[model_data["model_alias"]] = model_op

            # critical tests task subgroup
            if len(crit_tests_list) > 0:
                with TaskGroup(
                    group_id=f'{model_data["model_alias"]}_critical_tests'
                ) as crit_tests_task:
                    dbt_test_tasks = [
                        BashOperator(
                            task_id=test["test_alias"],
                            bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run.sh "
                            if test["test_type"] == "generic"
                            else f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test.sh ",
                            env={
                                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                                "target": "{{ params.target }}",
                                "model": f"{model_data['model_alias']}",
                                "full_ref_str": full_ref_str,
                                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                            },
                            append_env=True,
                            cwd=PATH_TO_DBT_PROJECT,
                        )
                        for test in crit_tests_list
                        if not test["test_alias"].endswith(
                            f'ref_{model_data["model_alias"]}_'
                        )
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
        return (model_op_dict,test_op_dict)
      
    dummy_expand = make_model_task.expand(simplified_manifest=buid_simplified_manifest(PATH_TO_DBT_TARGET))






    end = DummyOperator(task_id="end")

start >> dummy_expand  >> end
