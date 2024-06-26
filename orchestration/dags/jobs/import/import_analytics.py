import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.alerts import analytics_fail_slack_alert
from common.config import (
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    DAG_FOLDER,
    GCP_PROJECT_ID,
)
from common.operators.biquery import bigquery_job_task
from common.utils import depends_loop, get_airflow_schedule, waiting_operator
from dependencies.analytics.import_analytics import define_import_tables, export_tables

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 4,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_analytics_v7",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

wait_for_dbt = waiting_operator(dag=dag, dag_id="dbt_run_dag", external_task_id="end")

# TODO: remove legacy copy job
with TaskGroup(group_id="analytics_copy_group", dag=dag) as analytics_copy:
    import_tables_to_analytics_tasks = []
    for table in define_import_tables():
        task = BigQueryInsertJobOperator(
            dag=dag,
            task_id=f"import_to_analytics_{table}",
            configuration={
                "query": {
                    "query": f"SELECT * FROM `{GCP_PROJECT_ID}.{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}`",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ANALYTICS_DATASET,
                        "tableId": f"{APPLICATIVE_PREFIX}{table}",
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )
        import_tables_to_analytics_tasks.append(task)

end_import = DummyOperator(task_id="end_import", dag=dag)


start_analytics_table_tasks = DummyOperator(task_id="start_analytics_tasks", dag=dag)
analytics_table_jobs = {}
for table, job_params in export_tables.items():
    job_params["destination_table"] = job_params.get("destination_table", table)
    task = bigquery_job_task(dag=dag, table=table, job_params=job_params)
    analytics_table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
        "dag_depends": job_params.get("dag_depends", []),
    }


end_analytics_table_tasks = DummyOperator(task_id="end_analytics_table_tasks", dag=dag)
analytics_table_tasks = depends_loop(
    export_tables,
    analytics_table_jobs,
    start_analytics_table_tasks,
    dag,
    default_end_operator=end_analytics_table_tasks,
)

end = DummyOperator(task_id="end", dag=dag)

(start >> wait_for_dbt >> analytics_copy >> end_import >> start_analytics_table_tasks)
(analytics_table_tasks >> end_analytics_table_tasks >> end)
