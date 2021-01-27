import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.data_analytics.config import (
    GCP_PROJECT_ID,
    BIGQUERY_CLEAN_DATASET,
)
from dependencies.data_analytics.import_tables import define_import_query
from dependencies.data_analytics.anonymization import define_anonymization_query

# Variables
BIGQUERY_DATASET_NAME = "data_analytics"

from dependencies.slack_alert import task_fail_slack_alert

data_analytics_tables = [
    "user",
    "provider",
    "offerer",
    "bank_information",
    "booking",
    "payment",
    "venue",
    "user_offerer",
    "offer",
    "stock",
    "favorite",
    "venue_type",
    "venue_label",
    "payment_status",
    "iris_venues",
    "transaction",
    "local_provider_event",
    "beneficiary_import_status",
    "deposit",
    "beneficiary_import",
    "mediation",
    "iris_france",
    "user_offerer",
    "offer_criterion",
    "bank_information",
    "allocine_pivot",
    "venue_provider",
    "allocine_venue_provider_price_rule",
    "allocine_venue_provider",
    "payment_message",
    "provider",
    "feature",
    "criterion",
]

default_dag_args = {
    "on_failure_callback": task_fail_slack_alert,
    "start_date": datetime.datetime(2020, 11, 11),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_applicative_data_v1",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and populate the clean dataset for the data team",
    schedule_interval="0 6 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_tasks = []
for table in data_analytics_tables:
    task = BigQueryOperator(
        task_id=f"import_{table}",
        sql=define_import_query(table=table),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.applicative_database_{table}",
        dag=dag,
    )
    import_tables_tasks.append(task)

anonymization_task = BigQueryOperator(
    task_id="anonymization",
    sql=define_anonymization_query(dataset=BIGQUERY_CLEAN_DATASET),
    use_legacy_sql=False,
    dag=dag,
)
end = DummyOperator(task_id="end", dag=dag)

start >> import_tables_tasks >> anonymization_task >> end
