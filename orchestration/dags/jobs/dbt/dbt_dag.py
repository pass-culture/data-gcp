import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from common.config import PATH_TO_DBT_PROJECT
from airflow.exceptions import DuplicateTaskIdFound


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 23),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dbt_dag',
    default_args=default_args,
    description='A dbt wrapper for airflow',
    schedule_interval=timedelta(days=1),
)

def load_manifest():
    local_filepath = PATH_TO_DBT_PROJECT  + "/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)

    return data

# def make_dbt_task(node, dbt_verb,dag):
#     """Returns an Airflow operator either run and test an individual model"""
#     DBT_DIR = PATH_TO_DBT_PROJECT
#     GLOBAL_CLI_FLAGS = "--no-write-json"
#     model = node.split(".")[-1]

#     if dbt_verb == "run":
#         dbt_task = BashOperator(
#             task_id=node,
#             bash_command=f"""
#             dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
#             """,
#             dag=dag,
#             cwd=PATH_TO_DBT_PROJECT 
#         )

#     elif dbt_verb == "test":
#         node_test = node.replace("model", "test")
#         dbt_task = BashOperator(
#             task_id=node_test,
#             bash_command=f"""
#             dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
#             """,
#             dag=dag,
#             cwd=PATH_TO_DBT_PROJECT 
#         )

#     return dbt_task

def make_dbt_task(node,dag,dbt_verb="run"):
    """Returns an Airflow operator either run and test an individual model"""
    DBT_DIR = PATH_TO_DBT_PROJECT
    GLOBAL_CLI_FLAGS = "--no-write-json"
    
    prefix = node.split(".")[0]
    model_name = node.split(".")[-1]
    test_name = node.split(".")[-2]

    node_name = model_name if prefix == "model" else test_name if prefix == "test" else node

    # assert dbt_verb == "run", "dbt_verb other than run not implemented yet"
    
    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node_name,
            bash_command=f"""
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {node_name}
            """,
            cwd=PATH_TO_DBT_PROJECT, 
            dag=dag
        )
    else:
        print("dbt_verb other than run not implemented yet")
        return None
    return dbt_task


data = load_manifest()

dbt_tasks = {}
for node in data["nodes"].keys():
    if "elementary" not in node.split("."):
        if node.split(".")[0] in ["model","test"]:
            try :
                dbt_tasks[node] = make_dbt_task(node,dag,dbt_verb="run")
            except DuplicateTaskIdFound:
                print(node)
                # print(dbt_tasks[node])
                pass

# for node in data["nodes"].keys():
#     if node.split(".")[0] == "model":

#         # Set dependency to run tests on a model after model runs finishes
#         # set condition if test not empty in fact test declared in schema are nodes
#         # node_test = node.replace("model", "test")
#         dbt_tasks[node] >> dbt_tasks[node_test]

#         # Set all model -> model dependencies
#         for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:

#             upstream_node_type = upstream_node.split(".")[0]
#             if upstream_node_type == "model":
#                 dbt_tasks[upstream_node] >> dbt_tasks[node]

for node in data["nodes"].keys():
    # if node.split('.')[0] == 'test':
    #     upstream_nodes = tuple(data["nodes"][node]['depends_on']['nodes'])
    #     for unode in upstream_nodes:
    #         print( unode,node)
    #         dbt_tasks[unode] >> dbt_tasks[node]
    if "elementary" not in node.split("."):
        
        if node.split(".")[0] == "model": 

            # Set all model -> model dependencies
            for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    dbt_tasks[upstream_node] >> dbt_tasks[node]
                # from here build on critical tests dependancies
                