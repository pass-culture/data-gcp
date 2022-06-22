import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator
)

from airflow.operators.dummy_operator import DummyOperator

from dependencies.config import (
    BIGQUERY_BACKEND_DATASET,
    GCP_PROJECT,
)
#from dependencies.slack_alert import task_fail_slack_alert

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
    #on_failure_callback=task_fail_slack_alert,
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

delete_tables_tasks = []
create_tables_tasks = []
for table_name, request in tables_creation_requests.items():

    delete_task = BigQueryDeleteTableOperator(
        task_id=f"delete_table_{table_name}",
        deletion_dataset_table=f"{BIGQUERY_BACKEND_DATASET}.{table_name}",
        ignore_if_missing=True,
        dag=dag,
    )

    create_task = BigQueryExecuteQueryOperator(
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
