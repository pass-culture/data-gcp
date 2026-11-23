import copy
import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common import macros
from common.callback import on_failure_base_callback
from common.config import (
    BIGQUERY_LOCATION,
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_job_task
from common.utils import get_airflow_schedule
from dependencies.firebase.import_firebase import (
    BIGQUERY_TMP_DATASET_BY_LOCATION,
    SQL_PATH,
    import_perf_tables,
    import_tables,
)

dags = {
    # Reimport the data from the last two days
    "daily": {
        "prefix": "_intraday_",
        "schedule": "00 13 * * *",
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
        "schedule": "00 01 * * *",
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


def prepare_job_params(job_params, dag_type, yyyymmdd):
    """Return (query job params, final job params, staging table prefix).

    BigQuery cannot query across regions. When the source is not in the region of
    our datasets, the query writes into a staging table located in the source region;
    it is then copied to our region and loaded into the final table
    (see add_copy_and_load_tasks). Otherwise staging table prefix is None.
    """
    # suffixed with the run date so a run never overwrites the staging table of
    # another run still in progress (daily retries span more than a day)
    staging_table_prefix = f"{job_params['destination_table']}_{dag_type}"

    # force this to include custom yyyymmdd
    if job_params.get("partition_prefix", None) is not None:
        job_params["destination_table"] = (
            f"{job_params['destination_table']}{job_params['partition_prefix']}{yyyymmdd}"
        )
    final_job_params = job_params

    source_location = job_params.get("source_location", BIGQUERY_LOCATION)
    if source_location == BIGQUERY_LOCATION:
        return job_params, final_job_params, None

    query_job_params = dict(
        job_params,
        destination_dataset=BIGQUERY_TMP_DATASET_BY_LOCATION[source_location],
        destination_table=f"{staging_table_prefix}_" + "{{ ds_nodash }}",
        location=source_location,
        time_partitioning=None,
        clustering_fields=None,
        schemaUpdateOptions=None,
    )
    return query_job_params, final_job_params, staging_table_prefix


def add_copy_and_load_tasks(
    dag, job_name_table, query_job_params, final_job_params, staging_table_prefix
):
    """Copy the staging table to our region, then load it into the final table."""
    staging_table = query_job_params["destination_table"]
    copy_task = BigQueryInsertJobOperator(
        dag=dag,
        task_id=f"copy_{job_name_table}",
        project_id=GCP_PROJECT_ID,
        # cross-region copy jobs must run in the source location
        location=query_job_params["location"],
        configuration={
            "copy": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": query_job_params["destination_dataset"],
                    "tableId": staging_table,
                },
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": staging_table,
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )
    load_task = bigquery_job_task(
        dag=dag,
        table=f"load_{job_name_table}",
        job_params=dict(final_job_params, sql=f"{SQL_PATH}/raw/load_from_tmp.sql"),
        extra_params={"tmp_table_prefix": staging_table_prefix},
    )
    copy_task >> load_task
    return copy_task, load_task


for dag_type, params in dags.items():
    dag_id = f"import_{dag_type}_firebase_data"
    prefix = params["prefix"]
    yyyymmdd = params["yyyymmdd"]

    dag = DAG(
        dag_id,
        default_args=params["default_dag_args"],
        description="Import firebase data and dispatch it to each env",
        on_failure_callback=on_failure_base_callback,
        schedule=get_airflow_schedule(params["schedule"]),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=90),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
        tags=[DAG_TAGS.DE.value],
    )

    globals()[dag_id] = dag

    start = EmptyOperator(task_id="start", dag=dag)

    end = EmptyOperator(task_id="end", dag=dag)

    import_tables_temp = copy.deepcopy(import_tables)
    for job_name_table, job_params in import_tables_temp.items():
        job_params, final_job_params, staging_table_prefix = prepare_job_params(
            job_params, dag_type, yyyymmdd
        )

        if dag_type == "intraday":
            table_id = f"events{prefix}" + "{{ yyyymmdd(ds) }}"
        else:
            table_id = f"events{prefix}" + "{{ yyyymmdd(add_days(ds, -1)) }}"

        check_table_task = BranchPythonOperator(
            dag=dag,
            task_id=f"check_table_exists_task_{job_name_table}",
            python_callable=check_table_exists,
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

        end_job = EmptyOperator(
            task_id=f"end_{job_name_table}", dag=dag, trigger_rule="one_success"
        )

        start >> check_table_task >> [default_task, fallback_task] >> end_job

        if staging_table_prefix is None:
            end_job >> end
            continue

        copy_task, load_task = add_copy_and_load_tasks(
            dag, job_name_table, job_params, final_job_params, staging_table_prefix
        )
        end_job >> copy_task
        load_task >> end

    import_perf_tables_temp = copy.deepcopy(import_perf_tables)
    for job_name_table, job_params in import_perf_tables_temp.items():
        job_params, final_job_params, staging_table_prefix = prepare_job_params(
            job_params, dag_type, yyyymmdd
        )

        loading_task = bigquery_job_task(
            dag=dag,
            table=f"{job_name_table}",
            job_params=job_params,
            extra_params={"dag_type": dag_type},
        )

        start >> loading_task

        if staging_table_prefix is None:
            loading_task >> end
            continue

        copy_task, load_task = add_copy_and_load_tasks(
            dag, job_name_table, job_params, final_job_params, staging_table_prefix
        )
        loading_task >> copy_task
        load_task >> end
