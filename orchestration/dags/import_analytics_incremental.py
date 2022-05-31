import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    GCP_PROJECT,
)
from dependencies.data_analytics.enriched_data.booking import (
    define_enriched_booking_data_full_query,
)
from dependencies.data_analytics.enriched_data.collective_booking import (
    define_enriched_collective_booking_full_query,
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
from dependencies.data_analytics.enriched_data.venue_locations import (
    define_table_venue_locations,
)

from dependencies.data_analytics.import_tables import (
    define_import_query,
    define_replace_query,
)
from common.alerts import analytics_fail_slack_alert


def getting_service_account_token(function_name):
    function_url = (
        f"https://europe-west1-{GCP_PROJECT}.cloudfunctions.net/{function_name}"
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


# Variables
data_applicative_tables_and_date_columns = {
    "offer": ["offer_modified_at_last_provider_date", "offer_creation_date"],
}

default_dag_args = {
    "start_date": datetime.datetime(2022, 5, 30),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_data_analytics_incremental_v3",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase. "
    "This DAG import data incrementally",
    on_failure_callback=analytics_fail_slack_alert,
    schedule_interval=f"0 0 * * *",
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

start = DummyOperator(task_id="start", dag=dag)

offer_clean_duplicates = BigQueryExecuteQueryOperator(
    task_id="offer_clean_duplicates",
    sql=f"""
    SELECT * except(row_number)
    FROM (
        SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY offer_id
                                        ORDER BY offer_date_updated DESC
                                    ) as row_number
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}offer`
        )
    WHERE row_number=1
    """,
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}offer",
    dag=dag,
)

for table in data_applicative_tables_and_date_columns.keys():

    analytics_task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_analytics_{table}",
        sql=f"SELECT * {define_replace_query(data_applicative_tables_and_date_columns[table])} FROM {BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )

    import_offer_to_clean_tasks = []
    offer_task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_clean_{table}",
        sql=define_import_query(
            external_connection_id=APPLICATIVE_EXTERNAL_CONNECTION_ID,
            table=table,
        ),
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )
    end = DummyOperator(task_id="end", dag=dag)

    (start >> offer_task >> offer_clean_duplicates >> analytics_task >> end)
