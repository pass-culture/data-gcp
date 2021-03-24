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
    "import_data_analytics_v6",
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
create_enriched_booked_categories_data_v1_task = BigQueryOperator(
    task_id="create_enriched_booked_categories_data_v1",
    sql=f"""
        WITH bookings as (
            SELECT user_id, offer.offer_type,  venue_is_virtual FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_booking` booking
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_stock` stock
            ON booking.stock_id = stock.stock_id
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_offer` offer
            ON stock.offer_id = offer.offer_id
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_venue` venue
            ON venue.venue_id = offer.venue_id
        )
        select user_id,
        SUM(CAST(offer_type = 'ThingType.AUDIOVISUEL' AS INT64)) > 0 AS audiovisuel,
        SUM(CAST(offer_type in ('EventType.CINEMA', 'ThingType.CINEMA_ABO', 'ThingType.CINEMA_CARD') AS INT64)) > 0 AS cinema,
        SUM(CAST(offer_type in ('ThingType.JEUX_VIDEO_ABO', 'ThingType.JEUX_VIDEO') AS INT64)) > 0 AS jeux_videos,
        SUM(CAST(offer_type = 'ThingType.LIVRE_EDITION' or offer_type = 'ThingType.LIVRE_AUDIO' AS INT64)) > 0 AS livre,
        SUM(CAST(offer_type in ('EventType.MUSEES_PATRIMOINE', 'ThingType.MUSEES_PATRIMOINE_ABO') AS INT64)) > 0 AS musees_patrimoine,
        SUM(CAST(offer_type in ('EventType.MUSIQUE', 'ThingType.MUSIQUE_ABO') AS INT64)) > 0 AS musique,
        SUM(CAST(offer_type in ('EventType.PRATIQUE_ARTISTIQUE', 'ThingType.PRATIQUE_ARTISTIQUE_ABO') AS INT64)) > 0 AS pratique_artistique,
        SUM(CAST(offer_type in ('EventType.SPECTACLE_VIVANT', 'ThingType.SPECTACLE_VIVANT_ABO') AS INT64)) > 0 AS spectacle_vivant,
        SUM(CAST(offer_type = 'ThingType.INSTRUMENT' AS INT64)) > 0 AS instrument,
        SUM(CAST(offer_type = 'ThingType.PRESSE_ABO' AS INT64)) > 0 AS presse,
        SUM(CAST(offer_type in ('EventType.CONFERENCE_DEBAT_DEDICACE', 'ThingType.OEUVRE_ART', 'EventType.JEUX') AS INT64)) > 0 AS autre,
        FROM bookings
        group by user_id
    """,
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.enriched_booked_categories_v1",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_booked_categories_data_v2_task = BigQueryOperator(
    task_id="create_enriched_booked_categories_data_v2",
    sql=f"""
        WITH bookings as (
            SELECT user_id, offer.offer_type,  venue_is_virtual FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_booking` booking
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_stock` stock
            ON booking.stock_id = stock.stock_id
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_offer` offer
            ON stock.offer_id = offer.offer_id
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_venue` venue
            ON venue.venue_id = offer.venue_id
        )
        select user_id,
        SUM(CAST(offer_type in ('EventType.PRATIQUE_ARTISTIQUE', 'ThingType.PRATIQUE_ARTISTIQUE_ABO') AS INT64)) > 0 AS pratique_artistique,
        SUM(CAST(offer_type = 'EventType.CONFERENCE_DEBAT_DEDICACE' AS INT64)) > 0 AS autre,
        SUM(CAST(offer_type in ('EventType.MUSEES_PATRIMOINE', 'ThingType.MUSEES_PATRIMOINE_ABO') AS INT64)) > 0 AS musees_patrimoine,
        SUM(CAST(offer_type in ('EventType.SPECTACLE_VIVANT', 'ThingType.SPECTACLE_VIVANT_ABO') AS INT64)) > 0 AS spectacle_vivant,
        SUM(CAST(offer_type in ('EventType.MUSIQUE', 'ThingType.MUSIQUE_ABO', 'ThingType.MUSIQUE') AS INT64)) > 0 AS musique,
        SUM(CAST(offer_type in ('EventType.CINEMA', 'ThingType.CINEMA_CARD') AS INT64)) > 0 AS cinema,
        SUM(CAST(offer_type = 'ThingType.INSTRUMENT' AS INT64)) > 0 AS instrument,
        SUM(CAST(offer_type = 'ThingType.PRESSE_ABO' AS INT64)) > 0 AS presse,
        SUM(CAST(offer_type = 'ThingType.AUDIOVISUEL' AS INT64)) > 0 AS audiovisuel,
        SUM(CAST(offer_type in ('ThingType.JEUX_VIDEO_ABO', 'ThingType.JEUX_VIDEO') AS INT64)) > 0 AS jeux_videos,
        SUM(CAST(offer_type in ('ThingType.LIVRE_EDITION', 'ThingType.LIVRE_AUDIO') AS INT64)) > 0 AS livre,
        FROM bookings
        group by user_id
    """,
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

start >> import_tables_to_clean_tasks >> end_import_table_to_clean >> import_tables_to_analytics_tasks >> end_import >> create_enriched_data_tasks >> end
