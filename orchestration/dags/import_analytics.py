import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
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

from dependencies.tag_offers import extract_tags


def getting_service_account_token(function_name):
    function_url = (
        f"https://europe-west1-{GCP_PROJECT}.cloudfunctions.net/{function_name}"
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


# Variables
data_applicative_tables_and_date_columns = {
    "user": [
        "user_creation_date",
        "user_cultural_survey_filled_date",
        "user_last_connection_date",
    ],
    "provider": [""],
    "offerer": ["offerer_modified_at_last_provider_date", "offerer_creation_date"],
    "bank_information": ["dateModified"],
    "booking": [
        "booking_creation_date",
        "booking_used_date",
        "booking_cancellation_date",
    ],
    "payment": [""],
    "venue": ["venue_modified_at_last_provider", "venue_creation_date"],
    "user_offerer": [""],
    "offer": ["offer_modified_at_last_provider_date", "offer_creation_date"],
    "stock": [
        "stock_modified_at_last_provider_date",
        "stock_modified_date",
        "stock_booking_limit_date",
        "stock_creation_date",
    ],
    "favorite": ["dateCreated"],
    "venue_type": [""],
    "venue_label": [""],
    "payment_status": ["date"],
    "transaction": [""],
    "local_provider_event": ["date"],
    "beneficiary_import_status": ["date"],
    "deposit": ["dateCreated"],
    "beneficiary_import": [""],
    "mediation": ["dateCreated"],
    "offer_criterion": [""],
    "allocine_pivot": [""],
    "venue_provider": ["dateModifiedAtLastProvider"],
    "allocine_venue_provider_price_rule": [""],
    "allocine_venue_provider": [""],
    "payment_message": [""],
    "feature": [""],
    "criterion": [""],
}

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
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_to_clean_tasks = []
for table in data_applicative_tables_and_date_columns.keys():
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
for table in data_applicative_tables_and_date_columns.keys():
    task = BigQueryOperator(
        task_id=f"import_to_analytics_{table}",
        sql=f"SELECT * {define_replace_query(data_applicative_tables_and_date_columns[table])} FROM {BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
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
        analytics_dataset=BIGQUERY_ANALYTICS_DATASET,
        clean_dataset=BIGQUERY_CLEAN_DATASET,
        table_prefix=APPLICATIVE_PREFIX,
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

getting_downloads_service_account_token = PythonOperator(
    task_id="getting_downloads_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={
        "function_name": f"downloads_{ENV_SHORT_NAME}",
    },
    dag=dag,
)

import_downloads_data_to_bigquery = SimpleHttpOperator(
    task_id="import_downloads_data_to_bigquery",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"downloads_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_downloads_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)

create_enriched_app_downloads_stats = BigQueryOperator(
    task_id="create_enriched_app_downloads_stats",
    sql=f"SELECT * FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.app_downloads_stats`",
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.app_downloads_stats",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

getting_contentful_service_account_token = PythonOperator(
    task_id="getting_contentful_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={
        "function_name": f"contentful_{ENV_SHORT_NAME}",
    },
    dag=dag,
)

import_contentful_data_to_bigquery = SimpleHttpOperator(
    task_id="import_contentful_data_to_bigquery",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"contentful_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_contentful_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)

copy_playlists_to_analytics = BigQueryOperator(
    task_id="copy_playlists_to_analytics",
    sql=f"""
    SELECT * except(row_number)
    FROM (
        SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY tag
                                        ORDER BY date_updated DESC
                                    ) as row_number
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_criterion` c
        LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.contentful_data` d ON c.name = d.tag
        )
    WHERE row_number=1
    """,
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.applicative_database_criterion",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)


create_offer_extracted_data = BigQueryOperator(
    task_id="create_offer_extracted_data",
    sql=f"""SELECT offer_id, offer_type, LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.author"), " ")) AS author,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.performer")," ")) AS performer,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.musicType"), " ")) AS musicType,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.musicSubtype"), " ")) AS musicSubtype,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.stageDirector"), " ")) AS stageDirector,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.theater"), " ")) AS theater,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.showType"), " ")) AS showType,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.showSubType"), " ")) AS showSubType,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.speaker"), " ")) AS speaker,
             LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.rayon"), " ")) AS rayon
          FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_offer`""",
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.offer_extracted_data",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

end_enriched_data = DummyOperator(task_id="end_enriched_data", dag=dag)


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

(
    start
    >> import_tables_to_clean_tasks
    >> end_import_table_to_clean
    >> import_tables_to_analytics_tasks
    >> end_import
)
(
    end_import
    >> link_iris_venues_task
    >> copy_to_analytics_iris_venues
    >> create_enriched_data_tasks
    >> end_enriched_data
)
(
    end_enriched_data
    >> getting_service_account_token
    >> import_downloads_data_to_bigquery
    >> create_enriched_app_downloads_stats
    >> end
)
(
    create_enriched_data_tasks
    >> getting_contentful_service_account_token
    >> import_contentful_data_to_bigquery
    >> copy_playlists_to_analytics
    >> end
)
