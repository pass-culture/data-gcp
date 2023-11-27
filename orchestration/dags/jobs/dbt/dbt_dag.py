import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from common.config import PATH_TO_DBT_PROJECT
from airflow.exceptions import DuplicateTaskIdFound


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 23),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dbt_dag",
    default_args=default_args,
    description="A dbt wrapper for airflow",
    schedule_interval=None,
)


def load_manifest():
    local_filepath = PATH_TO_DBT_PROJECT + "/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data

def make_dbt_task(node, dbt_verb,dag):
    """Returns an Airflow operator either run and test an individual model"""
    DBT_DIR = PATH_TO_DBT_PROJECT
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=model if node.split(".")[0]=="model" else node.split(".")[0]+'.'+model,
            bash_command=f"""
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            cwd=PATH_TO_DBT_PROJECT, 
            dag=dag
        )

    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            cwd=PATH_TO_DBT_PROJECT,
            dag=dag
        )

    return dbt_task


data = load_manifest()

dbt_tasks = {}

for node in data["nodes"].keys():
    if "elementary" not in node.split("."):
        if node.split(".")[0] == "model":
            node_test = node.replace("model", "test")
            dbt_tasks[node] = make_dbt_task(node, "run",dag)
            try :
                dbt_tasks[node] = make_dbt_task(node, "run",dag)
            except DuplicateTaskIdFound:
                print(node)
                # print(dbt_tasks[node])
                pass
            try :
                dbt_tasks[node_test] = make_dbt_task(node, "test",dag)
            except DuplicateTaskIdFound:
                # print(node)
                # print(dbt_tasks[node])
                pass

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":

        # Set dependency to run tests on a model after model runs finishes
        # node_test = node.replace("model", "test")
        # dbt_tasks[node] >> dbt_tasks[node_test]

        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            if "elementary" not in upstream_node.split("."):
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    dbt_tasks[upstream_node] >> dbt_tasks[node]
