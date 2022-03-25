import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_BACKEND_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)
from dependencies.data_analytics.enriched_data.booking import (
    define_enriched_booking_data_full_query,
)
from dependencies.data_analytics.enriched_data.educational_booking import (
    define_enriched_educational_booking_full_query,
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
from dependencies.slack_alert import task_fail_slack_alert

tables_creation_requests = {
    "favorite_not_booked": """WITH favorites as (SELECT DISTINCT favorite.userId, offerId, offer.offer_subcategoryId as subcat, 
    (select count(*) from `passculture-data-ehp.analytics_dev.enriched_booking_data` where offer_subcategoryId = offer.offer_subcategoryId AND user_id = favorite.userId ) as user_bookings_for_this_subcat
FROM `passculture-data-ehp.analytics_dev.applicative_database_favorite` as favorite
LEFT JOIN `passculture-data-ehp.analytics_dev.enriched_booking_data` as booking 
ON favorite.userId = booking.user_id AND favorite.offerId = booking.offer_id
JOIN `passculture-data-ehp.analytics_dev.enriched_offer_data` as offer
ON favorite.offerId = offer.offer_id
WHERE dateCreated < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) 
AND booking.offer_id IS NULL AND booking.user_id IS NULL
AND offer.offer_is_bookable = True)
SELECT CURRENT_DATE() as table_creation_day, userId,
ARRAY_AGG(
        STRUCT(offerId, subcat, user_bookings_for_this_subcat)
        ORDER BY user_bookings_for_this_subcat DESC LIMIT 1
    )[OFFSET(0)].*
FROM favorites
GROUP BY userId"""
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "create_backend_tables",
    default_args=default_dag_args,
    description="Create daily tables for backend needs",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

delete_tables_tasks = []
create_tables_tasks = []
for table_name, request in tables_creation_requests.items():

    delete_task = BigQueryTableDeleteOperator(
        task_id=f"delete_table_{table_name}",
        deletion_dataset_table=f"{BIGQUERY_BACKEND_DATASET}.{table_name}",
        ignore_if_missing=True,
        dag=dag,
    )

    create_task = BigQueryOperator(
        task_id=f"create_table_{table_name}",
        sql=request,
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_BACKEND_DATASET}.{table_name}",
        dag=dag,
    )
    create_tables_tasks.append(create_task)

    delete_task >> create_task >> end


(start >> delete_task)
