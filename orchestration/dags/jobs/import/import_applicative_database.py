import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.utils import one_line_query, get_airflow_schedule
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.alerts import analytics_fail_slack_alert
from common.config import DAG_FOLDER
from common.operators.biquery import bigquery_job_task
from common.config import (
    GCP_PROJECT_ID,
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
)

from orchestration.dags.dependencies.applicative_database.import_applicative_database import (
    RAW_TABLES,
    HISTORICAL_CLEAN_APPLICATIVE_TABLES,
)

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 4,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_applicative_database",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

with TaskGroup(group_id="raw_operations_group", dag=dag) as raw_operations_group:
    import_tables_to_raw_tasks = []
    for table, params in RAW_TABLES.items():
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
                    "writeDisposition": params.get(
                        "write_disposition", "WRITE_TRUNCATE"
                    ),
                }
            },
            params=dict(params.get("params", {})),
            dag=dag,
        )
    import_tables_to_raw_tasks.append(task)


with TaskGroup(
    group_id="historical_applicative_group", dag=dag
) as historical_applicative:
    historical_data_applicative_tables_tasks = []
    for table, params in HISTORICAL_CLEAN_APPLICATIVE_TABLES.items():
        task = bigquery_job_task(dag, table, params)
        historical_data_applicative_tables_tasks.append(task)


end_raw = DummyOperator(task_id="end", dag=dag)

(start >> raw_operations_group >> historical_data_applicative_tables_tasks >> end_raw)
