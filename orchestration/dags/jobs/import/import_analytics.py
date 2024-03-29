import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.utils import depends_loop, get_airflow_schedule
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.operators.biquery import bigquery_job_task
from dependencies.import_analytics.import_analytics import export_tables
from dependencies.import_analytics.import_historical import (
    historical_clean_applicative_database,
    historical_analytics,
)
from common.alerts import analytics_fail_slack_alert
from common.config import DAG_FOLDER
from common.utils import waiting_operator

from common.config import (
    GCP_PROJECT_ID,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    APPLICATIVE_PREFIX,
)

from dependencies.import_analytics.import_clean import (
    clean_tables,
)
from dependencies.import_analytics.import_analytics import define_import_tables

import_tables = define_import_tables()

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

wait_for_raw = waiting_operator(dag=dag, dag_id="import_raw")

# CLEAN : Copier les tables Raw dans Clean sauf s'il y a une requete de transformation dans clean.

with TaskGroup(
    group_id="clean_transformations_group", dag=dag
) as clean_transformations:
    import_tables_to_clean_transformation_jobs = {}
    for table, params in clean_tables.items():
        task = bigquery_job_task(dag=dag, table=table, job_params=params)
        import_tables_to_clean_transformation_jobs[table] = {
            "operator": task,
            "depends": params.get("depends", []),
            "dag_depends": params.get("dag_depends", []),  # liste de dag_id
        }

    end_import_table_to_clean = DummyOperator(
        task_id="end_import_table_to_clean", dag=dag
    )

    import_tables_to_clean_transformation_tasks = depends_loop(
        clean_tables,
        import_tables_to_clean_transformation_jobs,
        wait_for_raw,
        dag,
        default_end_operator=end_import_table_to_clean,
    )


wait_for_clean_copy_dbt = waiting_operator(
    dag=dag, dag_id="dbt_run_dag", external_task_id="end"
)

end_import_table_to_clean = DummyOperator(task_id="end_import_table_to_clean", dag=dag)

start_historical_data_applicative_tables_tasks = DummyOperator(
    task_id="start_historical_data_applicative_tables_tasks", dag=dag
)

with TaskGroup(
    group_id="historical_applicative_group", dag=dag
) as historical_applicative:
    historical_data_applicative_tables_tasks = []
    for table, params in historical_clean_applicative_database.items():
        task = bigquery_job_task(dag, table, params)
        historical_data_applicative_tables_tasks.append(task)

end_historical_data_applicative_tables_tasks = DummyOperator(
    task_id="end_historical_data_applicative_tables_tasks", dag=dag
)

start_historical_analytics_table_tasks = DummyOperator(
    task_id="start_historical_analytics_table_tasks", dag=dag
)

with TaskGroup(
    group_id="historical_analytics_group", dag=dag
) as historical_analytics_group:
    historical_analytics_table_tasks = []
    for table, params in historical_analytics.items():
        task = bigquery_job_task(dag, table, params)
        historical_analytics_table_tasks.append(task)

end_historical_analytics_table_tasks = DummyOperator(
    task_id="end_historical_analytics_table_tasks", dag=dag
)


with TaskGroup(group_id="analytics_copy_group", dag=dag) as analytics_copy:
    import_tables_to_analytics_tasks = []
    for table in import_tables:
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
                    "writeDisposition": params.get(
                        "write_disposition", "WRITE_TRUNCATE"
                    ),
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
        "dag_depends": job_params.get("dag_depends", []),  # liste de dag_id
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

(
    start
    >> wait_for_raw
    >> clean_transformations
    >> wait_for_clean_copy_dbt
    >> analytics_copy
    >> end_import
)
(clean_transformations >> wait_for_clean_copy_dbt >> end_import_table_to_clean)

(
    end_import_table_to_clean
    >> start_historical_data_applicative_tables_tasks
    >> historical_applicative
    >> end_historical_data_applicative_tables_tasks
)
(
    end_historical_data_applicative_tables_tasks
    >> start_historical_analytics_table_tasks
    >> historical_analytics_group
    >> end_historical_analytics_table_tasks
)
(end_import >> start_analytics_table_tasks)
(analytics_table_tasks >> end_analytics_table_tasks)
(end_analytics_table_tasks, end_historical_analytics_table_tasks) >> end
