import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.data_analytics.config import (
    GCP_PROJECT_ID,
    GCP_REGION,
    EXTERNAL_CONNECTION_ID_VM,
)
from dependencies.data_analytics.import_tables import define_import_query
from dependencies.data_analytics.anonymization import define_anonymization_query
from dependencies.data_analytics.enriched_data.offer import (
    define_enriched_offer_data_full_query,
)
from dependencies.data_analytics.enriched_data.stock import (
    define_enriched_stock_data_full_query,
)
from dependencies.data_analytics.enriched_data.user import (
    define_enriched_user_data_full_query,
)
from dependencies.data_analytics.enriched_data.venue import (
    define_enriched_venue_data_full_query,
)
from dependencies.data_analytics.enriched_data.booking import (
    define_enriched_booking_data_full_query,
)
from dependencies.slack_alert import task_fail_slack_alert


# Variables
BIGQUERY_DATASET_NAME = "data_analytics"

data_applicative_tables = [
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
]

default_dag_args = {
    "on_failure_callback": task_fail_slack_alert,
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_data_analytics_v2",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Data Studio",
    schedule_interval="0 5 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

make_bq_dataset_task = BashOperator(
    task_id="make_bq_dataset",
    # Executing 'bq' command requires Google Cloud SDK which comes preinstalled in Cloud Composer.
    bash_command=f"bq ls {BIGQUERY_DATASET_NAME} || bq mk --dataset --location {GCP_REGION} {BIGQUERY_DATASET_NAME}",
    dag=dag,
)

import_tables_tasks = []
for table in data_applicative_tables:
    task = BigQueryOperator(
        task_id=f"import_{table}",
        sql=define_import_query(
            table=table, external_connection_id=EXTERNAL_CONNECTION_ID_VM
        ),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_DATASET_NAME}.{table}",
        dag=dag,
    )
    import_tables_tasks.append(task)

anonymization_task = BigQueryOperator(
    task_id="anonymization",
    sql=define_anonymization_query(dataset=BIGQUERY_DATASET_NAME, table_prefix=""),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_offer_data_task = BigQueryOperator(
    task_id="create_enriched_offer_data",
    sql=define_enriched_offer_data_full_query(dataset=BIGQUERY_DATASET_NAME),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_stock_data_task = BigQueryOperator(
    task_id="create_enriched_stock_data",
    sql=define_enriched_stock_data_full_query(dataset=BIGQUERY_DATASET_NAME),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_user_data_task = BigQueryOperator(
    task_id="create_enriched_user_data",
    sql=define_enriched_user_data_full_query(dataset=BIGQUERY_DATASET_NAME),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_venue_data_task = BigQueryOperator(
    task_id="create_enriched_venue_data",
    sql=define_enriched_venue_data_full_query(dataset=BIGQUERY_DATASET_NAME),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_booking_data_task = BigQueryOperator(
    task_id="create_enriched_booking_data",
    sql=define_enriched_booking_data_full_query(dataset=BIGQUERY_DATASET_NAME),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_data_tasks = [
    create_enriched_offer_data_task,
    create_enriched_stock_data_task,
    create_enriched_user_data_task,
    create_enriched_venue_data_task,
    create_enriched_booking_data_task,
]

end = DummyOperator(task_id="end", dag=dag)

start >> make_bq_dataset_task >> import_tables_tasks >> anonymization_task >> create_enriched_data_tasks >> end
