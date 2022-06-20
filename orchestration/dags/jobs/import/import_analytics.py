import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from common import macros
from dependencies.import_analytics.import_historical import (
    historical_clean_applicative_database,
    historical_analytics,
)

from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    APPLICATIVE_PREFIX,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)
from dependencies.import_analytics.enriched_data.booking import (
    define_enriched_booking_data_full_query,
)
from dependencies.import_analytics.enriched_data.collective_booking import (
    define_enriched_collective_booking_full_query,
)
from dependencies.import_analytics.enriched_data.offer import (
    define_enriched_offer_data_full_query,
)
from dependencies.import_analytics.enriched_data.collective_offer import (
    define_enriched_collective_offer_data_full_query,
)
from dependencies.import_analytics.enriched_data.offerer import (
    define_enriched_offerer_data_full_query,
)
from dependencies.import_analytics.enriched_data.stock import (
    define_enriched_stock_data_full_query,
)
from dependencies.import_analytics.enriched_data.user import (
    define_enriched_user_data_full_query,
)
from dependencies.import_analytics.enriched_data.deposit import (
    define_enriched_deposit_data_full_query,
)

from dependencies.import_analytics.enriched_data.institution import (
    define_enriched_institution_data_full_query,
)

from dependencies.import_analytics.enriched_data.venue import (
    define_enriched_venue_data_full_query,
)
from dependencies.import_analytics.enriched_data.venue_locations import (
    define_table_venue_locations,
)
from dependencies.import_analytics.import_tables import (
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
    "user": [
        "user_creation_date",
        "user_cultural_survey_filled_date",
        "user_last_connection_date",
    ],
    "provider": [""],
    "offerer": [
        "offerer_modified_at_last_provider_date",
        "offerer_creation_date",
        "offerer_validation_date",
    ],
    "offer": ["offer_modified_at_last_provider_date", "offer_creation_date"],
    "bank_information": ["dateModified"],
    "booking": [
        "booking_creation_date",
        "booking_used_date",
        "booking_cancellation_date",
    ],
    "user_suspension": ["eventDate"],
    "individual_booking": [""],
    "payment": [""],
    "venue": ["venue_modified_at_last_provider", "venue_creation_date"],
    "user_offerer": [""],
    "offer_report": [""],
    "stock": [
        "stock_modified_at_last_provider_date",
        "stock_modified_date",
        "stock_booking_limit_date",
        "stock_creation_date",
    ],
    "favorite": ["dateCreated"],
    "venue_type": [""],
    "venue_label": [""],
    "venue_contact": [""],
    "payment_status": ["date"],
    "cashflow": ["creationDate"],
    "cashflow_batch": ["creationDate"],
    "cashflow_log": [""],
    "cashflow_pricing": [""],
    "pricing": ["creationDate", "valueDate"],
    "pricing_line": [""],
    "pricing_log": [""],
    "business_unit": [""],
    "transaction": [""],
    "local_provider_event": ["date"],
    "beneficiary_import_status": ["date"],
    "deposit": ["dateCreated", "dateUpdated"],
    "recredit": ["dateCreated"],
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
    "beneficiary_fraud_review": ["datereviewed"],
    "beneficiary_fraud_check": [""],
    "educational_deposit": ["educational_deposit_creation_date"],
    "educational_institution": [""],
    "educational_redactor": [""],
    "educational_year": [
        "educational_year_beginning_date",
        "educational_year_expiration_date",
    ],
    "collective_booking": [
        "collective_booking_creation_date",
        "collective_booking_used_date",
        "collective_booking_cancellation_date",
        "collective_booking_cancellation_limit_date",
        "collective_booking_reimbursement_date",
        "collective_booking_confirmation_date",
        "collective_booking_confirmation_limit_date",
    ],
    "collective_offer": [
        "collective_offer_last_validation_date",
        "collective_offer_creation_date",
        "collective_offer_date_updated",
    ],
    "collective_offer_template": [
        "collective_offer_last_validation_date",
        "collective_offer_creation_date",
        "collective_offer_date_updated",
    ],
    "collective_stock": [
        "collective_stock_creation_date",
        "collective_stock_modification_date",
        "collective_stock_beginning_date_time",
        "collective_stock_booking_limit_date_time",
    ],
}


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_import_analytics_v7",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    on_failure_callback=analytics_fail_slack_alert,
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_to_clean_tasks = []
for table in data_applicative_tables_and_date_columns.keys():
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_clean_{table}",
        sql=define_import_query(
            external_connection_id=APPLICATIVE_EXTERNAL_CONNECTION_ID,
            table=table,
        ),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )
    import_tables_to_clean_tasks.append(task)


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


end_import_table_to_clean = DummyOperator(task_id="end_import_table_to_clean", dag=dag)

start_historical_data_applicative_tables_tasks = DummyOperator(
    task_id="start_historical_data_applicative_tables_tasks", dag=dag
)
historical_data_applicative_tables_tasks = []
for table, params in historical_clean_applicative_database.items():
    task = BigQueryExecuteQueryOperator(
        task_id=f"historical_{table}",
        sql=params["sql"],
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=params["destination_dataset_table"],
        time_partitioning=params.get("time_partitioning", None),
        cluster_fields=params.get("cluster_fields", None),
        dag=dag,
    )
    historical_data_applicative_tables_tasks.append(task)

end_historical_data_applicative_tables_tasks = DummyOperator(
    task_id="end_historical_data_applicative_tables_tasks", dag=dag
)

start_historical_analytics_table_tasks = DummyOperator(
    task_id="start_historical_analytics_table_tasks", dag=dag
)
historical_analytics_table_tasks = []
for table, params in historical_analytics.items():
    task = BigQueryExecuteQueryOperator(
        task_id=f"historical_{table}",
        sql=params["sql"],
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=params["destination_dataset_table"],
        time_partitioning=params.get("time_partitioning", None),
        cluster_fields=params.get("cluster_fields", None),
        dag=dag,
    )
    historical_analytics_table_tasks.append(task)

end_historical_analytics_table_tasks = DummyOperator(
    task_id="end_historical_analytics_table_tasks", dag=dag
)

import_tables_to_analytics_tasks = []
for table in data_applicative_tables_and_date_columns.keys():
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_to_analytics_{table}",
        sql=f"SELECT * {define_replace_query(data_applicative_tables_and_date_columns[table])} FROM {BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}{table}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{APPLICATIVE_PREFIX}{table}",
        dag=dag,
    )
    import_tables_to_analytics_tasks.append(task)

end_import = DummyOperator(task_id="end_import", dag=dag)

IRIS_DISTANCE = 50000

link_iris_venues_task = BigQueryExecuteQueryOperator(
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

copy_to_analytics_iris_venues = BigQueryExecuteQueryOperator(
    task_id=f"copy_to_analytics_iris_venues",
    sql=f"SELECT * FROM {BIGQUERY_CLEAN_DATASET}.iris_venues",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.iris_venues",
    dag=dag,
)

create_enriched_offer_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_offer_data",
    sql=define_enriched_offer_data_full_query(
        analytics_dataset=BIGQUERY_ANALYTICS_DATASET,
        clean_dataset=BIGQUERY_CLEAN_DATASET,
        table_prefix=APPLICATIVE_PREFIX,
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_collective_offer_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_collective_offer_data",
    sql=define_enriched_collective_offer_data_full_query(
        analytics_dataset=BIGQUERY_ANALYTICS_DATASET,
        clean_dataset=BIGQUERY_CLEAN_DATASET,
        table_prefix=APPLICATIVE_PREFIX,
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_stock_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_stock_data",
    sql=define_enriched_stock_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_user_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_user_data",
    sql=define_enriched_user_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)
create_enriched_deposit_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_deposit_data",
    sql=define_enriched_deposit_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_venue_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_venue_data",
    sql=define_enriched_venue_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_booking_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_booking_data",
    sql=define_enriched_booking_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_collective_booking_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_collective_booking_data",
    sql=define_enriched_collective_booking_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_institution_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_institution_data",
    sql=define_enriched_institution_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_enriched_offerer_data_task = BigQueryExecuteQueryOperator(
    task_id="create_enriched_offerer_data",
    sql=define_enriched_offerer_data_full_query(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
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

create_enriched_app_downloads_stats = BigQueryExecuteQueryOperator(
    task_id="create_enriched_app_downloads_stats",
    sql=f"""
    SELECT 
        date, 
        'apple' as provider, 
        sum(units) as total_downloads
    FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.apple_download_stats` 
    GROUP BY date
    UNION ALL
    SELECT 
        date, 
        'google' as provider, 
        sum(daily_device_installs) as total_downloads
    FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.google_download_stats` 
    GROUP BY date""",
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.app_downloads_stats",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

create_table_venue_locations = BigQueryExecuteQueryOperator(
    task_id="create_table_venue_locations",
    sql=define_table_venue_locations(
        dataset=BIGQUERY_ANALYTICS_DATASET, table_prefix=APPLICATIVE_PREFIX
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.venue_locations",
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

copy_playlists_to_analytics = BigQueryExecuteQueryOperator(
    task_id="copy_playlists_to_analytics",
    sql=f"""
    SELECT * except(row_number, tag)
    FROM (
        SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY name
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


create_offer_extracted_data = BigQueryExecuteQueryOperator(
    task_id="create_offer_extracted_data",
    sql=f"""SELECT offer_id, LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.author"), " ")) AS author,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.performer")," ")) AS performer,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.musicType"), " ")) AS musicType,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.musicSubtype"), " ")) AS musicSubtype,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.stageDirector"), " ")) AS stageDirector,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.showType"), " ")) AS showType,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.showSubType"), " ")) AS showSubType,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.speaker"), " ")) AS speaker,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.rayon"), " ")) AS rayon,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.theater.allocine_movie_id"), " ")) AS theater_movie_id,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.theater.allocine_room_id"), " ")) AS theater_room_id,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.type"), " ")) AS movie_type,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.visa"), " ")) AS visa,
                LOWER(TRIM(JSON_EXTRACT_SCALAR(offer_extra_data, "$.releaseDate"), " ")) AS releaseDate,
                LOWER(TRIM(JSON_EXTRACT(offer_extra_data, "$.genres"), " ")) AS genres,
                LOWER(TRIM(JSON_EXTRACT(offer_extra_data, "$.companies"), " ")) AS companies,
                LOWER(TRIM(JSON_EXTRACT(offer_extra_data, "$.countries"), " ")) AS countries,
                LOWER(TRIM(JSON_EXTRACT(offer_extra_data, "$.cast"), " ")) AS casting,
                LOWER(TRIM(JSON_EXTRACT(offer_extra_data, "$.isbn"), " ")) AS isbn,
            FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.applicative_database_offer`""",
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.offer_extracted_data",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

end_enriched_data = DummyOperator(task_id="end_enriched_data", dag=dag)
start_enriched_data = DummyOperator(task_id="start_enriched_data", dag=dag)


enriched_venues = [create_enriched_venue_data_task, create_table_venue_locations]
enriched_venues = [create_enriched_venue_data_task, create_table_venue_locations]


end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> import_tables_to_clean_tasks
    >> offer_clean_duplicates
    >> end_import_table_to_clean
    >> import_tables_to_analytics_tasks
    >> end_import
)
(
    end_import_table_to_clean
    >> start_historical_data_applicative_tables_tasks
    >> historical_data_applicative_tables_tasks
    >> end_historical_data_applicative_tables_tasks
)
(
    end_historical_data_applicative_tables_tasks
    >> start_historical_analytics_table_tasks
    >> historical_analytics_table_tasks
    >> end_historical_analytics_table_tasks
)
(
    end_import
    >> link_iris_venues_task
    >> copy_to_analytics_iris_venues
    >> create_offer_extracted_data
    >> start_enriched_data
)
(
    start_enriched_data
    >> create_enriched_stock_data_task
    >> create_enriched_offer_data_task
    >> create_enriched_collective_offer_data_task
    >> create_enriched_booking_data_task
    >> create_enriched_collective_booking_data_task
    >> create_enriched_user_data_task
    >> create_enriched_deposit_data_task
    >> enriched_venues
    >> create_enriched_offerer_data_task
    >> create_enriched_institution_data_task
    >> end_enriched_data
)
(
    end_enriched_data
    >> getting_downloads_service_account_token
    >> import_downloads_data_to_bigquery
    >> create_enriched_app_downloads_stats
    >> end
)
(
    end_enriched_data
    >> getting_contentful_service_account_token
    >> import_contentful_data_to_bigquery
    >> copy_playlists_to_analytics
    >> end
)
