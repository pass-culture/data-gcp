import datetime

from common import macros
from common.callback import on_failure_vm_callback
from common.config import DAG_FOLDER, DAG_TAGS, GCP_PROJECT_ID
from common.operators.bigquery import bigquery_job_task
from common.utils import depends_loop, get_airflow_schedule
from dependencies.propilote.export_propilote import propilote_tables

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_vm_callback,
}

dag = DAG(
    "export_propilote_data",
    default_args=default_dag_args,
    description="Export propilote date",
    schedule_interval=get_airflow_schedule("00 08 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DE.value],
)


start = DummyOperator(task_id="start", dag=dag)

table_jobs = {}
for table, job_params in propilote_tables.items():
    task = bigquery_job_task(dag, table, job_params)
    table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
        "dag_depends": job_params.get("dag_depends", []),
    }

end = DummyOperator(task_id="end", dag=dag)
table_jobs = depends_loop(
    propilote_tables, table_jobs, start, dag=dag, default_end_operator=end
)
