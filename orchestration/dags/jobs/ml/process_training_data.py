import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

from common import macros
from common.alerts import analytics_fail_slack_alert
from common.config import (
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT_ID,
)
from common.config import DAG_FOLDER
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

default_dag_args = {
    "start_date": datetime.datetime(2022, 7, 26),
    "retries": 1,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "process_training_data",
    default_args=default_dag_args,
    description="Import data for training and build aggregated tables",
    schedule_interval="0 10 * * 5",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)
training_data_tables = ["bookings", "clicks", "favorites"]
aggregated_tables = ["users", "offers"]
enriched_tables = ["users"]
import_tables_to_raw_tasks = []
for table in training_data_tables:
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_raw_training_data_{table}",
        sql=(IMPORT_TRAINING_SQL_PATH / f"training_data_{table}.sql").as_posix(),
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
        sql=(
            IMPORT_TRAINING_SQL_PATH / f"training_data_aggregated_{table}.sql"
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.training_data_aggregated_{table}",
        dag=dag,
    )
    aggregated_tables_to_clean_tasks.append(task)
enriched_tasks = []
for table in enriched_tables:
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_clean_enriched_{table}",
        sql=(
            IMPORT_TRAINING_SQL_PATH / f"training_data_enriched_{table}.sql"
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.training_data_enriched_{table}",
        dag=dag,
    )
    enriched_tasks.append(task)

end_import_to_raw = DummyOperator(task_id="end_import_to_raw", dag=dag)
end_import_to_clean = DummyOperator(task_id="end_import_to_clean", dag=dag)
end_enriched_task = DummyOperator(task_id="end_enriched_task", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> import_tables_to_raw_tasks
    >> end_import_to_raw
    >> aggregated_tables_to_clean_tasks
    >> end_import_to_clean
    >> enriched_tasks
    >> end_enriched_task
    >> end
)
