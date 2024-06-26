import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.alerts import analytics_fail_slack_alert
from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    DAG_FOLDER,
    GCP_PROJECT_ID,
)
from common.operators.biquery import bigquery_job_task
from common.utils import get_airflow_schedule, one_line_query
from dependencies.applicative_database.import_applicative_database import (
    HISTORICAL_CLEAN_APPLICATIVE_TABLES,
    RAW_TABLES,
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
                    "query": f"""SELECT * FROM EXTERNAL_QUERY('{APPLICATIVE_EXTERNAL_CONNECTION_ID}', ''' {one_line_query(params['sql'])} ''')""",
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

middle = DummyOperator(task_id="import", dag=dag)

with TaskGroup(
    group_id="historical_applicative_group", dag=dag
) as historical_applicative:
    historical_data_applicative_tables_tasks = []
    for table, params in HISTORICAL_CLEAN_APPLICATIVE_TABLES.items():
        task = bigquery_job_task(dag, table, params)
        historical_data_applicative_tables_tasks.append(task)


end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> raw_operations_group
    >> middle
    >> historical_data_applicative_tables_tasks
    >> end
)
