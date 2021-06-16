import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    GCP_PROJECT,
)
from dependencies.data_analytics.enriched_data.booked_categories import (
    enrich_booked_categories,
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
from dependencies.data_analytics.import_tables import (
    define_import_query,
    define_replace_query,
)
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
    "transaction",
    "local_provider_event",
    "beneficiary_import_status",
    "deposit",
    "beneficiary_import",
    "mediation",
    "offer_criterion",
    "allocine_pivot",
    "venue_provider",
    "allocine_venue_provider_price_rule",
    "allocine_venue_provider",
    "payment_message",
    "feature",
    "criterion",
]

date_to_convert = [
    [
        "user_creation_date",
        "user_cultural_survey_filled_date",
        "user_last_connection_date",
    ],
    "",
    ["offerer_modified_at_last_provider_date", "offerer_creation_date"],
    "dateModified",
    ["booking_creation_date", "booking_used_date", "booking_cancellation_date"],
    "",
    ["venue_modified_at_last_provider", "venue_creation_date"],
    "",
    ["offer_modified_at_last_provider_date", "offer_creation_date"],
    [
        "stock_modified_at_last_provider_date",
        "stock_modified_date",
        "stock_booking_limit_date",
        "stock_creation_date",
    ],
    "dateCreated",
    "",
    "",
    "date",
    "",
    "date",
    "date",
    "dateCreated",
    "",
    "dateCreated",
    "",
    "",
    "dateModifiedAtLastProvider",
    "",
    "",
    "",
    "",
    "",
    "",
]

tables_to_convert = dict(zip(data_applicative_tables, date_to_convert))

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_data_analytics_v6",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Data Studio",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 23 * * *",
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
        sql=f"SELECT * {define_replace_query(tables_to_convert[table])} FROM {BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )
    import_tables_to_analytics_tasks.append(task)

end_import = DummyOperator(task_id="end_import", dag=dag)

IRIS_DISTANCE = 100000

link_iris_venues_task = BigQueryOperator(
    task_id="link_iris_venues_task",
    sql=f"""
    WITH venues_to_link AS (
        SELECT venue_id, venue_longitude, venue_latitude
        FROM `{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}venue` as venue
        JOIN  `{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}offerer` as offerer ON venue_managing_offerer_id=offerer_id
        LEFT JOIN `{BIGQUERY_CLEAN_DATASET}.iris_venues` as iv on venue.venue_id = iv.venueId
        WHERE iv.venueId is null
        AND venue_is_virtual is false
        AND venue_validation_token is null
        AND offerer_validation_token is null
    )
    SELECT iris_france.id as irisId, venue_id as venueId FROM {BIGQUERY_CLEAN_DATASET}.iris_france, venues_to_link
    WHERE ST_DISTANCE(centroid, ST_GEOGPOINT(venue_longitude, venue_latitude)) < {IRIS_DISTANCE}
    """,
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.iris_venues",
    write_disposition="WRITE_APPEND",
    use_legacy_sql=False,
    dag=dag,
)

copy_to_analytics_iris_venues = BigQueryOperator(
    task_id=f"copy_to_analytics_iris_venues",
    sql=f"SELECT * FROM {BIGQUERY_CLEAN_DATASET}.iris_venues",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.iris_venues",
    dag=dag,
)

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

create_enriched_booked_categories_data_v1_task = BigQueryOperator(
    task_id="create_enriched_booked_categories_data_v1",
    sql=enrich_booked_categories(
        gcp_project=GCP_PROJECT,
        bigquery_analytics_dataset=BIGQUERY_ANALYTICS_DATASET,
        version=1,
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.enriched_booked_categories_v1",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_booked_categories_data_v2_task = BigQueryOperator(
    task_id="create_enriched_booked_categories_data_v2",
    sql=enrich_booked_categories(
        gcp_project=GCP_PROJECT,
        bigquery_analytics_dataset=BIGQUERY_ANALYTICS_DATASET,
        version=2,
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.enriched_booked_categories_v2",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_data_tasks = [
    create_enriched_offer_data_task,
    create_enriched_stock_data_task,
    create_enriched_user_data_task,
    create_enriched_venue_data_task,
    create_enriched_booking_data_task,
    create_enriched_booked_categories_data_v1_task,
    create_enriched_booked_categories_data_v2_task,
    create_enriched_offerer_data_task,
]

end = DummyOperator(task_id="end", dag=dag)

start >> import_tables_to_clean_tasks >> end_import_table_to_clean >> import_tables_to_analytics_tasks >> end_import
end_import >> link_iris_venues_task >> copy_to_analytics_iris_venues >> create_enriched_data_tasks >> end
