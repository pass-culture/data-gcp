import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from common import macros
from common.utils import depends_loop, one_line_query


from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from dependencies.import_analytics.import_analytics import export_tables
from dependencies.import_analytics.import_historical import (
    historical_clean_applicative_database,
    historical_analytics,
)
from common.alerts import analytics_fail_slack_alert
from common.config import DAG_FOLDER

from common.config import (
    GCP_PROJECT_ID,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    APPLICATIVE_PREFIX,
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
)

from dependencies.import_analytics.import_raw import (
    get_tables_config_dict,
    RAW_SQL_PATH,
)
from dependencies.import_analytics.import_clean import (
    clean_tables,
    get_clean_tables_copy_dict,
)
from dependencies.import_analytics.import_analytics import define_import_tables

import_tables = define_import_tables()
clean_tables_copy = get_clean_tables_copy_dict()
raw_tables = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH, BQ_DESTINATION_DATASET=BIGQUERY_RAW_DATASET
)


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_analytics_v7",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

# RAW

with TaskGroup(group_id="raw_operations_group", dag=dag) as raw_operations_group:
    import_tables_to_raw_tasks = []
    for table, params in raw_tables.items():
        task = BigQueryInsertJobOperator(
            task_id=f"import_to_raw_{table}",
            configuration={
                "query": {
                    "query": f"""SELECT * FROM EXTERNAL_QUERY('{APPLICATIVE_EXTERNAL_CONNECTION_ID}', '{one_line_query(params['sql'])}')""",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": params["destination_dataset"],
                        "tableId": params["destination_table"],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            params=dict(params.get("params", {})),
            dag=dag,
        )
    import_tables_to_raw_tasks.append(task)


end_raw = DummyOperator(task_id="end_raw", dag=dag)

# CLEAN : Copier les tables Raw dans Clean sauf s'il y a une requete de transformation dans clean.

with TaskGroup(
    group_id="clean_transformations_group", dag=dag
) as clean_transformations:

    import_tables_to_clean_transformation_tasks = []
    for table, params in clean_tables.items():
        task = BigQueryInsertJobOperator(
            task_id=f"import_to_clean_{table}",
            configuration={
                "query": {
                    "query": "{% include '" + params["sql"] + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": params["destination_dataset"],
                        "tableId": params["destination_table"],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            params=dict(params.get("params", {})),
            dag=dag,
        )
        import_tables_to_clean_transformation_tasks.append(task)


with TaskGroup(group_id="clean_copy_group", dag=dag) as clean_copy:
    import_tables_to_clean_copy_tasks = []
    for table, params in clean_tables_copy.items():
        task = BigQueryInsertJobOperator(
            task_id=f"import_to_clean_{table}",
            configuration={
                "query": {
                    "query": params["sql"],
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": params["destination_dataset"],
                        "tableId": params["destination_table"],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            params=dict(params.get("params", {})),
            dag=dag,
        )
        import_tables_to_clean_copy_tasks.append(task)

end_import_table_to_clean = DummyOperator(task_id="end_import_table_to_clean", dag=dag)

start_historical_data_applicative_tables_tasks = DummyOperator(
    task_id="start_historical_data_applicative_tables_tasks", dag=dag
)

with TaskGroup(
    group_id="historical_applicative_group", dag=dag
) as historical_applicative:
    historical_data_applicative_tables_tasks = []
    for table, params in historical_clean_applicative_database.items():
        task = BigQueryExecuteQueryOperator(
            task_id=f"historical_{table}",
            sql=params["sql"],
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=params["destination_dataset_table"],
            time_partitioning=params.get("time_partitioning", None),
            cluster_fields=params.get("cluster_fields", None),
            dag=dag,
        )
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
        task = BigQueryExecuteQueryOperator(
            task_id=f"historical_{table}",
            sql=params["sql"],
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=params["destination_dataset_table"],
            time_partitioning=params.get("time_partitioning", None),
            cluster_fields=params.get("cluster_fields", None),
            dag=dag,
        )
        historical_analytics_table_tasks.append(task)

end_historical_analytics_table_tasks = DummyOperator(
    task_id="end_historical_analytics_table_tasks", dag=dag
)


with TaskGroup(group_id="analytics_copy_group", dag=dag) as analytics_copy:
    import_tables_to_analytics_tasks = []
    for table in import_tables:
        task = BigQueryExecuteQueryOperator(
            task_id=f"import_to_analytics_{table}",
            sql=f"SELECT * FROM {BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{APPLICATIVE_PREFIX}{table}",
            dag=dag,
        )
        import_tables_to_analytics_tasks.append(task)

end_import = DummyOperator(task_id="end_import", dag=dag)


start_analytics_table_tasks = DummyOperator(task_id="start_analytics_tasks", dag=dag)
analytics_table_jobs = {}
for table, job_params in export_tables.items():
    destination_table = job_params.get("destination_table", table)
    task = BigQueryInsertJobOperator(
        task_id=table,
        configuration={
            "query": {
                "query": "{% include '" + job_params["sql"] + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": job_params["destination_dataset"],
                    "tableId": destination_table,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "timePartitioning": job_params.get("time_partitioning", None),
                "clustering": job_params.get("clustering_fields", None),
            },
        },
        trigger_rule=job_params.get("trigger_rule", "all_success"),
        params=dict(job_params.get("params", {})),
        dag=dag,
    )

    analytics_table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
    }

analytics_table_tasks = depends_loop(analytics_table_jobs, start_analytics_table_tasks)
end_analytics_table_tasks = DummyOperator(task_id="end_analytics_table_tasks", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> raw_operations_group
    >> end_raw
    >> clean_transformations
    >> end_import_table_to_clean
    >> analytics_copy
    >> end_import
)
(end_raw >> clean_copy >> end_import_table_to_clean)
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
(analytics_table_tasks >> end_analytics_table_tasks >> end)
