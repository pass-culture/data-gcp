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
    GCP_PROJECT,
)
from common.config import DAG_FOLDER
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH


default_dag_args = {
    "start_date": datetime.datetime(2022, 7, 26),
    "retries": 1,
    "on_failure_callback": analytics_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

with DAG(
    "process_training_data",
    default_args=default_dag_args,
    description="Import data for training and build aggregated tables",
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    import_to_raw_training_dataset = BigQueryExecuteQueryOperator(
        task_id=f"import_to_raw_training_dataset",
        sql=(IMPORT_TRAINING_SQL_PATH / f"tmp/training_dataset_clicks.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.training_dataset_clicks",
        dag=dag,
    )

    import_to_raw_evaluation_dataset = BigQueryExecuteQueryOperator(
        task_id=f"import_to_raw_evaluation_dataset",
        sql=(IMPORT_TRAINING_SQL_PATH / f"tmp/evaluation_dataset_clicks.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.evaluation_dataset_clicks",
        dag=dag,
    )

    import_to_raw_test_dataset = BigQueryExecuteQueryOperator(
        task_id=f"import_to_raw_test_dataset",
        sql=(IMPORT_TRAINING_SQL_PATH / f"tmp/test_dataset_clicks.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.test_dataset_clicks",
        dag=dag,
    )

    end = DummyOperator(task_id="start", dag=dag)

    (
        start
        >> import_to_raw_training_dataset
        >> import_to_raw_evaluation_dataset
        >> import_to_raw_test_dataset
        >> end
    )
