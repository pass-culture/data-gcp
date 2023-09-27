import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, DAG_FOLDER
from common.alerts import task_fail_slack_alert
from dependencies.backend.create_tables import create_tables
from common import macros
from common.operators.biquery import bigquery_job_task
from common.utils import get_airflow_schedule, depends_loop


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}


dag_schedule = {"daily": "00 01 * * *", "weekly": "00 01 * * 1"}
for schedule_type, schedule_cron in dag_schedule.items():
    dag_id = f"export_backend_tables_{schedule_type}"
    dag = DAG(
        dag_id,
        default_args=default_dag_args,
        description=f"Export {schedule_type} tables for backend needs",
        on_failure_callback=task_fail_slack_alert,
        schedule_interval=get_airflow_schedule(schedule_cron),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=120),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
    )
    globals()[dag_id] = dag

    start = DummyOperator(task_id="start", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    export_table_jobs = {}
    for table, params in create_tables.items():
        if params["schedule_type"] == schedule_type:
            task = bigquery_job_task(dag, table, params)
            export_table_jobs[table] = {
                "operator": task,
                "depends": params.get("depends", []),
                "dag_depends": params.get("dag_depends", []),
            }

    export_table_tasks = (
        depends_loop(
            create_tables, export_table_jobs, start, dag, default_end_operator=end
        ),
    )

    (start >> export_table_tasks)
