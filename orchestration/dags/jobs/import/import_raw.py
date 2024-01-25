import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.utils import one_line_query, get_airflow_schedule
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.alerts import analytics_fail_slack_alert
from common.config import DAG_FOLDER

from common.config import (
    GCP_PROJECT_ID,
    BIGQUERY_RAW_DATASET,
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
)

from dependencies.import_analytics.import_raw import (
    get_tables_config_dict,
    RAW_SQL_PATH,
)

raw_tables = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + RAW_SQL_PATH, BQ_DESTINATION_DATASET=BIGQUERY_RAW_DATASET
)

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 4,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_raw",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
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
                    "writeDisposition": params.get(
                        "write_disposition", "WRITE_TRUNCATE"
                    ),
                }
            },
            params=dict(params.get("params", {})),
            dag=dag,
        )
    import_tables_to_raw_tasks.append(task)


end_raw = DummyOperator(task_id="end_raw", dag=dag)

(start >> raw_operations_group >> end_raw)
