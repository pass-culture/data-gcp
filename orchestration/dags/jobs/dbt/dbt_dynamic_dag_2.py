import datetime
import json

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import datetime, timedelta
from airflow.models import Param
from common.alerts import task_fail_slack_alert
from common.utils import (
    get_airflow_schedule,
)
# from common.dbt.utils import rebuild_manifest

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

### Functions to import from common.dbt.utils
import os
def load_json_artifact(_PATH_TO_DBT_TARGET,artifact):
    local_filepath = _PATH_TO_DBT_TARGET + "/" + artifact
    with open(local_filepath) as f:
        data = json.load(f)
    return data

def load_manifest(_PATH_TO_DBT_TARGET):
    return load_json_artifact(_PATH_TO_DBT_TARGET,"manifest.json")

def get_resources_list(manifest,resource_type,profile_or_package_name = None):  
    resource_list = [ node for node in manifest['nodes'].keys() if (
            (True if resource_type == None else (manifest["nodes"][node]["resource_type"] == resource_type)) and (True if profile_or_package_name == None else profile_or_package_name in node)
        )]
    return resource_list


manifest = load_json_artifact(PATH_TO_DBT_TARGET,"manifest.json")

### tasks
# @task(task_id="load_manifest")
# def load_manifest(_PATH_TO_DBT_TARGET):
#     return {"manifest":load_json_artifact(_PATH_TO_DBT_TARGET,"manifest.json")} #implicit xcom push


@task(task_id="models_tasks")
def get_models():
    # manifest = ti.xcom_pull(key="manifest", task_ids="load_manifest")
    return get_resources_list(manifest,"model",profile_or_package_name = "data_gcp_dbt")

@task(task_id="models_tests")
def get_tests():
    return get_resources_list(manifest,"test",profile_or_package_name = "data_gcp_dbt")

@task
def run_model(node):
    full_ref_str = " --full-refresh" if "{{ params.full_refresh|lower }}" == 'true' else ""
    model_op = BashOperator(
                task_id=manifest["nodes"][node]["alias"],
                bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run.sh ",
                env={
                    "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                    "target": "{{ params.target }}",
                    "model": manifest["nodes"][node]["alias"],
                    "full_ref_str": full_ref_str,
                    "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                },
                append_env=True,
                cwd=PATH_TO_DBT_PROJECT,
            )
    return model_op

#### dag

with DAG(
    "aaa_dbt_dynamic_dag",
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

    @task(task_id="load_manifest")
    def load_manifest(_PATH_TO_DBT_TARGET):
        return {"manifest":load_json_artifact(_PATH_TO_DBT_TARGET,"manifest.json")} #implicit xcom push

    @task(task_id="fetch_models_tasks")
    def get_models():
        # manifest = ti.xcom_pull(key="manifest", task_ids="load_manifest")
        return get_resources_list(manifest,"model",profile_or_package_name = "data_gcp_dbt") #implicit xcom push
    
    @task(task_id="fetch_tests_tasks")
    def get_tests(ti):
        # manifest = ti.xcom_pull(key="manifest", task_ids="load_manifest")
        return get_resources_list(manifest,"test",profile_or_package_name = "data_gcp_dbt") #implicit xcom push

    @task
    def run_model(ti,node):
        manifest = ti.xcom_pull(key="manifest", task_ids="load_manifest")
        full_ref_str = " --full-refresh" if "{{ params.full_refresh|lower }}" == 'true' else ""
        model_op = BashOperator(
                    task_id=manifest["nodes"][node]["alias"],
                    bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run.sh ",
                    env={
                        "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                        "target": "{{ params.target }}",
                        "model": manifest["nodes"][node]["alias"],
                        "full_ref_str": full_ref_str,
                        "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                    },
                    append_env=True,
                    cwd=PATH_TO_DBT_PROJECT,
                )
        return model_op

    manifest_task = load_manifest(PATH_TO_DBT_TARGET,do_xcom_push=True)
    model_nodes_task = get_models(manifest_task.output)
    models_tasks = run_model.partial(manifest=manifest_task.output).expand(node=model_nodes_task.output)
    end = DummyOperator(task_id="end")

start >> models_tasks >> end


