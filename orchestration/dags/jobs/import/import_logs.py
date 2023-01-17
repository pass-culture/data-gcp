import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from dependencies.logs.import_logs import (
    import_tables,
)
from common.operators.biquery import bigquery_job_task
from common.config import DAG_FOLDER, GCP_PROJECT_ID
from common.alerts import task_fail_slack_alert


default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 20),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_logs",
    default_args=default_dag_args,
    description="Import tables from log sink",
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)


start_import_logs_task = DummyOperator(task_id="start_import_logs_task", dag=dag)
import_logs_tasks = []
for table, params in import_tables.items():
    task = bigquery_job_task(dag=dag, table=table, job_params=params)
    import_logs_tasks.append(task)

end_import_logs_task = DummyOperator(task_id="end_import_logs_task", dag=dag)


start_import_logs_task >> import_logs_tasks >> end_import_logs_task
