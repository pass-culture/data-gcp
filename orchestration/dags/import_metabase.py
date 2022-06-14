import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from dependencies.metabase.import_metabase import import_tables, from_external
from dependencies.config import METABASE_EXTERNAL_CONNECTION_ID
from dependencies.config import (
    GCP_PROJECT,
)
from common.alerts import task_fail_slack_alert


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_metabase",
    default_args=default_dag_args,
    description="Import metabase tables from CloudSQL",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_to_raw_tasks = []
for name, params in import_tables.items():

    task = BigQueryExecuteQueryOperator(
        task_id=f"import_metabase_{name}_to_raw",
        sql=from_external(
            conn_id=METABASE_EXTERNAL_CONNECTION_ID,
            params=params,
        ),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=params["destination_dataset_table"],
        dag=dag,
    )
    import_tables_to_raw_tasks.append(task)

end = DummyOperator(task_id="end", dag=dag)


start >> import_tables_to_raw_tasks >> end
