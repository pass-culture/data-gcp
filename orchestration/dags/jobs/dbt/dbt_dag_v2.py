import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from common.config import PATH_TO_DBT_PROJECT
from airflow.exceptions import DuplicateTaskIdFound
from airflow.models import Param

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 23),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dbt_dag2",
    default_args=default_args,
    description="A dbt wrapper for airflow - with critical test nodes",
    schedule_interval=None,
    params={
        "target": Param(
            default="dev",
            type="string",
        )
    },
)


def load_manifest():
    local_filepath = PATH_TO_DBT_PROJECT + "/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data

def get_children_tests(data):
    children_tests = {node:{"redirect_dep":None, "model_alias":data["nodes"][node]["alias"],"depends_on_node":data["nodes"][node]["depends_on"]["nodes"],"model_tests":{},"resource_type":data["nodes"][node]["resource_type"]} for node in data["nodes"].keys() if (data["nodes"][node]["resource_type"] == "model" and "elementary" not in node)}
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
                
                if children_tests[p_node]['model_tests'].get(test_config,None) is None:
                    children_tests[p_node]['model_tests'][test_config] = [{"test_alias":test_alias,'test_node':node,"test_type":'generic' if generic_test else 'custom'}]
                else:
                     children_tests[p_node]['model_tests'][test_config] += [{"test_alias":test_alias,'test_node':node,"test_type":'generic' if generic_test else 'custom'}]

    return children_tests



DBT_DIR = PATH_TO_DBT_PROJECT
GLOBAL_CLI_FLAGS = "--no-write-json"
full_refresh = False

start = DummyOperator(task_id="start",dag=dag)


dbt_compile_op = BashOperator(
        task_id="run_compile_dbt",
        bash_command="dbt deps && dbt compile --target {{ params.target }} --vars '{\"ENV_SHORT_NAME\": \"dev\"}' ",
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag
    )


    
with TaskGroup(group_id='data_transformation',dag=dag) as data_transfo:

    data = load_manifest()
    children_tests = get_children_tests(data)


    for model_node,model_data in children_tests.items():
        tests_list = model_data["model_tests"].get('error',[])

        if len(tests_list) != 0 :   
            with TaskGroup(group_id=f'{model_data["model_alias"]}_tasks',dag=dag) as model_tasks:
                model_op = BashOperator(
                    task_id = model_data['model_alias'],
                    bash_command=f"""
                    dbt {GLOBAL_CLI_FLAGS} run --target {{ params.target }} --select {model_data['model_alias']} --no-compile
                    """ if not full_refresh else f"""
                    dbt {GLOBAL_CLI_FLAGS} run --target {{ params.target }} --select {model_data['model_alias']} --no-compile --full-refresh
                    """,
                    cwd=PATH_TO_DBT_PROJECT, 
                    dag=dag
                    )

                with TaskGroup(group_id=f'{model_data["model_alias"]}_critical_tests',dag=dag) as tests_task:
                    dbt_test_tasks = [BashOperator(
                    task_id = test['test_alias'],
                    bash_command=f"""
                    dbt {GLOBAL_CLI_FLAGS} run --target {{ params.target }} --select {test['test_alias']} --no-compile
                    """ if not full_refresh else f"""
                    dbt {GLOBAL_CLI_FLAGS} run --target {{ params.target }} --select {test['test_alias']} --no-compile --full-refresh
                    """,
                    cwd=PATH_TO_DBT_PROJECT, 
                    dag=dag
                    ) for test in tests_list]
                model_op >> tests_task
        else:
            model_tasks = BashOperator(
                    task_id = f'{model_data["model_alias"]}_tasks',
                    bash_command=f"""
                    dbt {GLOBAL_CLI_FLAGS} run --target {{ params.target }} --select {model_data['model_alias']} --no-compile
                    """ if not full_refresh else f"""
                    dbt {GLOBAL_CLI_FLAGS} run --target {{ params.target }} --select {model_data['model_alias']} --no-compile --full-refresh
                    """,
                    cwd=PATH_TO_DBT_PROJECT, 
                    dag=dag
                    )
   
        children_tests[model_node]["redirect_dep"] = model_tasks


    for node in children_tests.keys():
        for upstream_node in children_tests[node]["depends_on_node"]:
            if upstream_node is not None:
                if upstream_node.startswith("model."):
                    try:
                        children_tests[upstream_node]['redirect_dep'] >> children_tests[node]['redirect_dep']
                    except:
                        pass
                else:
                    pass
   

end = DummyOperator(task_id='transfo_completed',dag=dag)

start >> dbt_compile_op >> data_transfo >> end