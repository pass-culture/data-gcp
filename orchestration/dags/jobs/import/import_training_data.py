from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import datetime
from common.alerts import analytics_fail_slack_alert
from common import macros
from common.config import DAG_FOLDER

IMPORT_TRAINING_SQL_PATH = f"dependencies/import_training_data/sql"
from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)


default_dag_args = {
    "start_date": datetime.datetime(2022, 7, 26),
    "retries": 1,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_training_data",
    default_args=default_dag_args,
    description="Import data for training and build aggregated tables",
    schedule_interval="0 10 * * 5",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)
training_data_tables = ["bookings", "clics", "favorites"]
aggregated_tables = ["users","offers"]
import_tables_to_raw_tasks = []
for table in training_data_tables:
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_raw_training_data_{table}",
        sql=f"{IMPORT_TRAINING_SQL_PATH}/{table}.sql",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.training_data_{table}",
        dag=dag,
    )
    import_tables_to_raw_tasks.append(task)

aggregated_tables_to_clean_tasks = []
for table in aggregated_tables:
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_clean_aggregated_{table}",
        sql=f"{IMPORT_TRAINING_SQL_PATH}/aggregated_{table}.sql",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.training_data_aggregated_{table}",
        dag=dag,
    )
    aggregated_tables_to_clean_tasks.append(task)

end_import_to_raw = DummyOperator(task_id="end_import_to_raw", dag=dag)
end_import_to_clean = DummyOperator(task_id="end_import_to_clean", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> import_tables_to_raw_tasks
    >> end_import_to_raw
    >> aggregated_tables_to_clean_tasks
    >> end_import_to_clean
    >> end
)
