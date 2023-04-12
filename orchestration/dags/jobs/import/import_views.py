import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from common.operators.biquery import bigquery_view_task
from dependencies.views.import_views import import_tables

from common.config import DAG_FOLDER, GCP_PROJECT_ID
from common.alerts import task_fail_slack_alert
from common.utils import get_airflow_schedule

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 20),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_views",
    default_args=default_dag_args,
    description="Import tables from log sink",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)


start_task = DummyOperator(task_id="start", dag=dag)
tasks = []
for table, params in import_tables.items():
    task = bigquery_view_task(dag=dag, table=table, job_params=params)
    tasks.append(task)

end_task = DummyOperator(task_id="end", dag=dag)


start_task >> tasks >> end_task
