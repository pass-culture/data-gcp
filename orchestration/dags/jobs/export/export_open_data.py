import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from dependencies.open_data.export_open_data import aggregated_open_data_tables
from common.config import DAG_FOLDER, GCP_PROJECT_ID
from common.alerts import task_fail_slack_alert
from common.utils import get_airflow_schedule

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    "export_open_data",
    default_args=default_dag_args,
    description="Export aggregated tables for open data",
    schedule_interval=get_airflow_schedule("00 08 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)


start_export_open_data_task = DummyOperator(
    task_id="start_export_open_data_task", dag=dag
)
export_tasks = []
for table, params in aggregated_open_data_tables.items():
    task = BigQueryExecuteQueryOperator(
        task_id=f"export_{table}",
        sql=params["sql"],
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=params["destination_dataset_table"],
        time_partitioning=params.get("time_partitioning", None),
        cluster_fields=params.get("cluster_fields", None),
        dag=dag,
    )
    export_tasks.append(task)

end_export_open_data_task = DummyOperator(task_id="end_export_open_data_task", dag=dag)


start_export_open_data_task >> export_tasks >> end_export_open_data_task
