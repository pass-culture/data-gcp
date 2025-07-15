import copy
import datetime

from common import macros
from common.callback import on_failure_base_callback
from common.config import DAG_FOLDER, DAG_TAGS, GCP_PROJECT_ID
from common.operators.bigquery import bigquery_job_task
from common.utils import get_airflow_schedule
from dependencies.firebase.import_firebase import import_tables, import_perf_tables

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

dags = {
    # Reimport the data from the last two days
    "daily": {
        "prefix": "_intraday_",
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
    # Import data from the last day
    "intraday": {
        "prefix": "_intraday_",
        "schedule_interval": "00 01 * * *",
        # one day ago
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 3,
            "retry_delay": datetime.timedelta(minutes=30),
            "project_id": GCP_PROJECT_ID,
        },
    },
}


def check_table_exists(**kwargs):
    import logging

    job_params = kwargs["job_params"]
    job_name = kwargs["job_name"]
    table = kwargs["table"]
    bq_hook = BigQueryHook()
    client = bq_hook.get_client(project_id=GCP_PROJECT_ID)
    for dataset_id in job_params.get("gcp_project_env", []):
        logging.info(f"Check if {table} exists in {dataset_id}")
        try:
            client.get_table(f"{dataset_id}.{table}")
        except Exception:
            logging.info(f"Not found {table} in {dataset_id}, error. Fallback.")
            return f"fallback_{job_name}"
    logging.info(f"Everything is fine, proceed with default_{job_name}")
    return f"default_{job_name}"


for dag_type, params in dags.items():
    dag_id = f"import_{dag_type}_firebase_data"
    prefix = params["prefix"]
    yyyymmdd = params["yyyymmdd"]

    dag = DAG(
        dag_id,
        default_args=params["default_dag_args"],
        description="Import firebase data and dispatch it to each env",
        on_failure_callback=on_failure_base_callback,
        schedule_interval=get_airflow_schedule(params["schedule_interval"]),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=90),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
        tags=[DAG_TAGS.DE.value],
    )

    globals()[dag_id] = dag

    start = DummyOperator(task_id="start", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    import_tables_temp = copy.deepcopy(import_tables)
    for job_name_table, job_params in import_tables_temp.items():
        # force this to include custom yyyymmdd
        if job_params.get("partition_prefix", None) is not None:
            job_params["destination_table"] = (
                f"{job_params['destination_table']}{job_params['partition_prefix']}{yyyymmdd}"
            )

        if dag_type == "intraday":
            table_id = f"events{prefix}" + "{{ yyyymmdd(ds) }}"
        else:
            table_id = f"events{prefix}" + "{{ yyyymmdd(add_days(ds, -1)) }}"

        check_table_task = BranchPythonOperator(
            dag=dag,
            task_id=f"check_table_exists_task_{job_name_table}",
            python_callable=check_table_exists,
            provide_context=True,
            op_kwargs={
                "job_params": job_params["params"],
                "table": table_id,
                "job_name": job_name_table,
            },
        )

        default_task = bigquery_job_task(
            dag=dag,
            table=f"default_{job_name_table}",
            job_params=job_params,
            extra_params={"prefix": prefix, "dag_type": dag_type},
        )

        fallback_task = bigquery_job_task(
            dag=dag,
            table=f"fallback_{job_name_table}",
            # overwrite default configuration, execute only if the default task fails
            job_params=job_params,
            extra_params=dict(
                {"prefix": prefix, "dag_type": dag_type},
                **job_params.get("fallback_params", {}),
            ),
        )

        end_job = DummyOperator(
            task_id=f"end_{job_name_table}", dag=dag, trigger_rule="one_success"
        )

        start >> check_table_task >> [default_task, fallback_task] >> end_job >> end

    import_perf_tables_temp = copy.deepcopy(import_perf_tables)
    for job_name_table, job_params in import_perf_tables_temp.items():
        if job_params.get("partition_prefix", None) is not None:
            job_params["destination_table"] = (
                f"{job_params['destination_table']}{job_params['partition_prefix']}{yyyymmdd}"
            )

        loading_task = bigquery_job_task(
            dag=dag,
            table=f"{job_name_table}",
            job_params=job_params,
            extra_params={"dag_type": dag_type},
        )

        start >> loading_task >> end
