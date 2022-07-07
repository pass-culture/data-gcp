import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator

from common.config import GCP_PROJECT
from common.alerts import task_fail_slack_alert
from dependencies.backend.create_tables import create_tables
from common import macros
from common.config import DAG_FOLDER

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "export_backend_tables",
    default_args=default_dag_args,
    description="Export daily tables for backend needs",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

export_table_tasks = []
for table, params in create_tables.items():
    task = BigQueryExecuteQueryOperator(
        task_id=f"export_{table}",
        sql=params["sql"],
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
        destination_dataset_table=params["destination_dataset_table"],
        time_partitioning=params.get("time_partitioning", None),
        cluster_fields=params.get("cluster_fields", None),
        dag=dag,
    )
    export_table_tasks.append(task)


(start >> export_table_tasks >> end)
