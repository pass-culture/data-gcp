import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from common.config import PATH_TO_DBT_PROJECT, ENV_SHORT_NAME
from airflow.exceptions import DuplicateTaskIdFound
from airflow.models import Param
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)

from common import macros
from common.config import GCP_PROJECT_ID, PATH_TO_DBT_PROJECT, ENV_SHORT_NAME


def load_manifest():
    local_filepath = PATH_TO_DBT_PROJECT + "/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data

def build_simplified_manifest(data):
    simplified_manifest = {node:{"redirect_dep":None, "model_alias":data["nodes"][node]["alias"],"depends_on_node":data["nodes"][node]["depends_on"]["nodes"],"model_tests":{},"resource_type":data["nodes"][node]["resource_type"]} for node in data["nodes"].keys() if (data["nodes"][node]["resource_type"] == "model" and "elementary" not in node)}
    for node in data["nodes"].keys():
        if data["nodes"][node]["resource_type"] == "test":
            generic_test = True in [generic_name in node for generic_name in ["not_null","unique","accepted_values","relationships"]]
            test_alias =  data["nodes"][node]["alias"] if not generic_test else node.split('.')[-2]
            test_config = data["nodes"][node]["config"].get('severity',None)
            try:
                test_config = test_config.lower()
            except AttributeError:
                pass
            parents = data["nodes"][node]["depends_on"]["nodes"]
            for p_node in parents:
                # p_alias  = data["nodes"][p_node]["alias"]
                
                if simplified_manifest[p_node]['model_tests'].get(test_config,None) is None:
                    simplified_manifest[p_node]['model_tests'][test_config] = [{"test_alias":test_alias,'test_node':node,"test_type":'generic' if generic_test else 'custom'}]
                else:
                     simplified_manifest[p_node]['model_tests'][test_config] += [{"test_alias":test_alias,'test_node':node,"test_type":'generic' if generic_test else 'custom'}]

    return simplified_manifest

def rebuild_manifest():
    data = load_manifest()
    simplified_manifest = build_simplified_manifest(data)
    return simplified_manifest


default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "dbt_data_quality_tests",
    default_args=default_args,
    description="A dbt wrapper for airflow",
    schedule_interval=None,
    params={
        "target": Param(
            default="dev",
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
    }   
)
external_tasks_sensor = {}
simplified_manifest = rebuild_manifest()


start = DummyOperator(task_id="start",dag=dag)
alerting_task = DummyOperator(task_id="dummy_alerting_task",dag=dag)
end = DummyOperator(task_id="end",dag=dag)


for model_node,model_data in simplified_manifest.items():
        tests_list = model_data["model_tests"].get('warn',[])
        if len(tests_list) != 0 :   
            with TaskGroup(group_id=f'{model_data["model_alias"]}_quality_tests',dag=dag) as model_testing_task:
                # first create waiting task for an external model execution
                external_tasks_sensor[model_node] = ExternalTaskSensor(
                    task_id=f'{model_data["model_alias"]}_external_task_sensor',
                    poke_interval=60,
                    timeout=180,
                    soft_fail=False,
                    retries=2,
                    external_task_id=model_data['model_alias'],
                    external_dag_id='dbt_run_dag',
                    dag=dag)
                #then create a group of test task 
                with TaskGroup(group_id=f'{model_data["model_alias"]}_critical_tests',dag=dag) as tests_tasks:
                    dbt_test_tasks = [BashOperator(
                    task_id = test['test_alias'],
                    bash_command=f"""
                    dbt {{ params.GLOBAL_CLI_FLAGS }} run --target {{ params.target }} --select {test['test_alias']} --no-compile
                    """ if (not "{{ params.full_refresh }}" and test['test_type'] == 'generic') else f"""
                    dbt {{ params.GLOBAL_CLI_FLAGS }} run --target {{ params.target }} --select {test['test_alias']} --no-compile --full-refresh
                    """ if ("{{ params.full_refresh }}" and test['test_type'] == 'generic') else f"""
                    dbt {{ params.GLOBAL_CLI_FLAGS }} test --target {{ params.target }} --select {test['test_alias']} --no-compile
                    """ if (not "{{ params.full_refresh }}" and test['test_type'] == 'custom') else f"""
                    dbt {{ params.GLOBAL_CLI_FLAGS }} test --target {{ params.target }} --select {test['test_alias']} --no-compile --full-refresh
                    """,
                    cwd=PATH_TO_DBT_PROJECT, 
                    dag=dag
                    ) for test in tests_list]
                    external_tasks_sensor[model_node] >> tests_tasks
                
                start >> model_testing_task >> alerting_task

alerting_task >> end

# TO DO : gather test warnings logs and send them to slack alert task through xcom 

