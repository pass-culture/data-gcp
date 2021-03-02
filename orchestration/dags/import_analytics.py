import datetime
import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)
from dependencies.data_analytics.enriched_data.booking import (
    define_enriched_booking_data_full_query,
)
from dependencies.data_analytics.enriched_data.offer import (
    define_enriched_offer_data_full_query,
)
from dependencies.data_analytics.enriched_data.offerer import (
    define_enriched_offerer_data_full_query,
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
from dependencies.data_analytics.import_tables import define_import_query
from dependencies.slack_alert import task_fail_slack_alert

# Variables
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
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_data_analytics_v5",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Data Studio",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 5 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_to_clean_tasks = []
for table in data_applicative_tables:
    task = BigQueryOperator(
        task_id=f"import_to_clean_{table}",
        sql=define_import_query(
            external_connection_id=APPLICATIVE_EXTERNAL_CONNECTION_ID, table=table
        ),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )
    import_tables_to_clean_tasks.append(task)

end_import_table_to_clean = DummyOperator(task_id="end_import_table_to_clean", dag=dag)

import_tables_to_analytics_tasks = []
for table in data_applicative_tables:
    task = BigQueryOperator(
        task_id=f"import_to_analytics_{table}",
        sql=f"SELECT * FROM {BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )
    import_tables_to_analytics_tasks.append(task)

end_import = DummyOperator(task_id="end_import", dag=dag)

create_enriched_offer_data_task = BigQueryOperator(
    task_id="create_enriched_offer_data",
    sql=define_enriched_offer_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_stock_data_task = BigQueryOperator(
    task_id="create_enriched_stock_data",
    sql=define_enriched_stock_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_user_data_task = BigQueryOperator(
    task_id="create_enriched_user_data",
    sql=define_enriched_user_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_venue_data_task = BigQueryOperator(
    task_id="create_enriched_venue_data",
    sql=define_enriched_venue_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_booking_data_task = BigQueryOperator(
    task_id="create_enriched_booking_data",
    sql=define_enriched_booking_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_offerer_data_task = BigQueryOperator(
    task_id="create_enriched_offerer_data",
    sql=define_enriched_offerer_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_data_tasks = [
    create_enriched_offer_data_task,
    create_enriched_stock_data_task,
    create_enriched_user_data_task,
    create_enriched_venue_data_task,
    create_enriched_booking_data_task,
    create_enriched_offerer_data_task,
]

end = DummyOperator(task_id="end", dag=dag)

start >> import_tables_to_clean_tasks >> end_import_table_to_clean >> import_tables_to_analytics_tasks >> end_import >> create_enriched_data_tasks >> end
