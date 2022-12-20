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
from dependencies.import_analytics.import_tables import get_raw_table_dict
from common.alerts import analytics_fail_slack_alert

# from common.utils import from_external

from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    GCP_PROJECT,
    DAG_FOLDER,
)

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    # "retries": 1,
    # "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}


def one_line_query(sql_path):
    with open(f"{sql_path}", "r") as fp:
        lines = " ".join([line.strip() for line in fp.readlines()])
    return lines


dag = DAG(
    "import_raw_test",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_to_clean_tasks = []
raw_tables = get_raw_table_dict()

for table, params in raw_tables.items():
    task = BigQueryInsertJobOperator(
        task_id=f"import_to_raw_{table}",
        configuration={
            "query": {
                "query": f"""SELECT * FROM EXTERNAL_QUERY('{APPLICATIVE_EXTERNAL_CONNECTION_ID}', '{one_line_query(params['sql'])}')""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": params["destination_dataset"],
                    "tableId": params["destination_table"],
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        params=dict(params.get("params", {})),
        dag=dag,
    )
    import_tables_to_clean_tasks.append(task)


start >> import_tables_to_clean_tasks
