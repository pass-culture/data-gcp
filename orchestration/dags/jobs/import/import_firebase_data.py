import datetime
from common import macros
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from common.utils import depends_loop
from common.operators.biquery import bigquery_job_task
from airflow.operators.dummy_operator import DummyOperator
from common.config import DAG_FOLDER
from common.config import (
    GCP_PROJECT_ID,
)
from dependencies.firebase.import_firebase import import_tables
from common.alerts import task_fail_slack_alert


dags = {
    "daily": {
        "prefix": "_",
        "schedule_interval": "00 13 * * *",
        # two days ago
        "yyyymmdd": "{{ yyyymmdd(add_days(ds, -1)) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 6,
            "retry_delay": datetime.timedelta(hours=6),
            "project_id": GCP_PROJECT_ID,
        },
    },
    "intraday": {
        "prefix": "_intraday_",
        "schedule_interval": "00 03 * * *",
        # one day ago
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 1,
            "retry_delay": datetime.timedelta(hours=6),
            "project_id": GCP_PROJECT_ID,
        },
    },
}


for type, params in dags.items():
    dag_id = f"import_{type}_firebase_data"
    prefix = params["prefix"]
    yyyymmdd = params["yyyymmdd"]

    dag = DAG(
        dag_id,
        default_args=params["default_dag_args"],
        description="Import firebase data and dispatch it to each env",
        on_failure_callback=task_fail_slack_alert,
        schedule_interval=params["schedule_interval"],
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=90),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
    )

    globals()[dag_id] = dag

    start = DummyOperator(task_id="start", dag=dag)
    table_jobs = {}
    for table, job_params in import_tables.items():
        # force this to include custom yyyymmdd
        if job_params.get("partition_prefix", None) is not None:
            job_params[
                "destination_table"
            ] = f"{job_params['destination_table']}{job_params['partition_prefix']}{yyyymmdd}"
        task = bigquery_job_task(
            dag=dag,
            table=table,
            job_params=job_params,
            extra_params={"prefix": prefix, "dag_type": type},
        )

        table_jobs[table] = {
            "operator": task,
            "depends": job_params.get("depends", []),
        }

    table_jobs = depends_loop(table_jobs, start)
    end = DummyOperator(task_id="end", dag=dag)
    table_jobs >> end
