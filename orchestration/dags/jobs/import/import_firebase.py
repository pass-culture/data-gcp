import copy
import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, GCP_PROJECT_ID
from common.operators.biquery import bigquery_job_task
from common.utils import get_airflow_schedule
from dependencies.firebase.import_firebase import import_tables

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
        "schedule_interval": "00 01 * * *",
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
        schedule_interval=get_airflow_schedule(params["schedule_interval"]),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=90),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
    )

    globals()[dag_id] = dag
    # Cannot Schedule before 3UTC for intraday and 13UTC for daily
    start = DummyOperator(task_id="start", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    import_tables_temp = copy.deepcopy(import_tables)
    for table, job_params in import_tables_temp.items():
        if type == "daily" and import_tables_temp[table].get("dag_depends") is not None:
            del import_tables_temp[table]["dag_depends"]
        # force this to include custom yyyymmdd
        if job_params.get("partition_prefix", None) is not None:
            job_params["destination_table"] = (
                f"{job_params['destination_table']}{job_params['partition_prefix']}{yyyymmdd}"
            )

        default_task = bigquery_job_task(
            dag=dag,
            table=table,
            job_params=job_params,
            extra_params={"prefix": prefix, "dag_type": type},
        )

        fallback_task = bigquery_job_task(
            dag=dag,
            table=f"{table}_fallback",
            # overwrite default configuration, execute only if the default task fails
            job_params=dict(job_params, **{"trigger_rule": "one_failed"}),
            extra_params=dict(
                {"prefix": prefix, "dag_type": type},
                **job_params.get("fallback_params", {}),
            ),
        )

        end_job = DummyOperator(
            task_id=f"end_{table}", dag=dag, trigger_rule="one_success"
        )

        start >> default_task >> [fallback_task, end_job]
        fallback_task >> end_job >> end
