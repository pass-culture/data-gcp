import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from dependencies.propilote.export_propilote import (
    propilote_tables,
)
from common.config import DAG_FOLDER, GCP_PROJECT_ID
from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task
from common.utils import depends_loop

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    "export_propilote_data",
    default_args=default_dag_args,
    description="Export propilote date",
    schedule_interval="00 08 * * 1,3,5",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)


start = DummyOperator(task_id="start", dag=dag)

table_jobs = {}
for table, job_params in propilote_tables.items():
    task = bigquery_job_task(dag, table, job_params)
    table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
    }

table_jobs = depends_loop(table_jobs, start)
end = DummyOperator(task_id="end", dag=dag)
table_jobs >> end
