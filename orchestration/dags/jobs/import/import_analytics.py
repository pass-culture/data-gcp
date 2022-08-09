import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from common.utils import depends_loop

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
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    GCP_PROJECT,
)

from dependencies.import_analytics.import_tables import (
    define_import_query,
    define_replace_query,
    define_import_tables,
)

import_tables = define_import_tables()

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_analytics_v7",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_to_clean_tasks = []
for table in import_tables.keys():
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_clean_{table}",
        sql=define_import_query(
            external_connection_id=APPLICATIVE_EXTERNAL_CONNECTION_ID,
            table=table,
        ),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )
    import_tables_to_clean_tasks.append(task)


offer_clean_duplicates = BigQueryExecuteQueryOperator(
    task_id="offer_clean_duplicates",
    sql=f"""
    SELECT * except(row_number)
    FROM (
        SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY offer_id
                                        ORDER BY offer_date_updated DESC
                                    ) as row_number
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}offer`
        )
    WHERE row_number=1
    """,
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}offer",
    dag=dag,
)


end_import_table_to_clean = DummyOperator(task_id="end_import_table_to_clean", dag=dag)

start_historical_data_applicative_tables_tasks = DummyOperator(
    task_id="start_historical_data_applicative_tables_tasks", dag=dag
)
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

import_tables_to_analytics_tasks = []
for table in import_tables.keys():
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_analytics_{table}",
        sql=f"SELECT * {define_replace_query(import_tables[table])} FROM {BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
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
                    "projectId": GCP_PROJECT,
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
    >> import_tables_to_clean_tasks
    >> offer_clean_duplicates
    >> end_import_table_to_clean
    >> import_tables_to_analytics_tasks
    >> end_import
)
(
    end_import_table_to_clean
    >> start_historical_data_applicative_tables_tasks
    >> historical_data_applicative_tables_tasks
    >> end_historical_data_applicative_tables_tasks
)
(
    end_historical_data_applicative_tables_tasks
    >> start_historical_analytics_table_tasks
    >> historical_analytics_table_tasks
    >> end_historical_analytics_table_tasks
)
(end_import >> start_analytics_table_tasks)
(analytics_table_tasks >> end_analytics_table_tasks >> end)
