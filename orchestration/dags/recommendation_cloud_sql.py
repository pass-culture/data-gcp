import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLImportInstanceOperator,
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from dependencies.access_gcp_secrets import access_secret_data
from dependencies.compose_gcs_files import compose_gcs_files
from dependencies.config import (
    GCP_PROJECT,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)
from common.alerts import task_fail_slack_alert

LOCATION = os.environ.get("REGION")

RECOMMENDATION_SQL_INSTANCE = os.environ.get("RECOMMENDATION_SQL_INSTANCE")
RECOMMENDATION_SQL_BASE = os.environ.get("RECOMMENDATION_SQL_BASE")

database_url = access_secret_data(
    GCP_PROJECT, f"{RECOMMENDATION_SQL_BASE}-database-url", default=""
)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    database_url.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT}&location={LOCATION}&instance={RECOMMENDATION_SQL_INSTANCE}&use_proxy=True&sql_proxy_use_tcp=True"
)


TABLES_DATA_PATH = f"{os.environ.get('DAG_FOLDER')}/tables.csv"
BUCKET_PATH = f"gs://{DATA_GCS_BUCKET_NAME}/bigquery_exports"

default_args = {
    "start_date": datetime(2020, 12, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
FIREBASE_PERIOD_DAYS = 4 * 30 if ENV_SHORT_NAME == "prod" else 10


def get_table_data():
    data = {}
    tables = pd.read_csv(TABLES_DATA_PATH)
    for table_name in set(tables["table_name"].values):
        data[table_name] = {}
        table_data = tables.loc[lambda df: df.table_name == table_name]
        data[table_name]["columns"] = {
            column_name: data_type
            for column_name, data_type in zip(
                list(table_data.column_name.values),
                list(table_data.data_type.values),
            )
        }
        for additional_data in ["dataset_type", "bigquery_table_name"]:
            data[table_name][additional_data] = tables.loc[
                lambda df: df.table_name == table_name
            ][additional_data].values[0]
    return data


TABLES = get_table_data()


def get_table_names():
    tables = pd.read_csv(TABLES_DATA_PATH)
    table_names = tables.table_name.unique()
    return table_names


with DAG(
    "recommendation_cloud_sql_v1",
    default_args=default_args,
    description="Export bigQuery tables to GCS to dump and restore Cloud SQL tables",
    schedule_interval="30 3 * * *",
    on_failure_callback=task_fail_slack_alert,
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
) as dag:

    start = DummyOperator(task_id="start")

    start_drop_restore = DummyOperator(task_id="start_drop_restore")
    end_data_prep = DummyOperator(task_id="end_data_prep")

    firebase_start_date = (
        datetime.now() - timedelta(days=FIREBASE_PERIOD_DAYS)
    ).strftime("%Y-%m-%d")
    firebase_end_date = datetime.now().strftime("%Y-%m-%d")
    for table in TABLES:

        dataset_type = TABLES[table]["dataset_type"]
        bigquery_table_name = TABLES[table]["bigquery_table_name"]
        if dataset_type == "clean":
            dataset = BIGQUERY_CLEAN_DATASET
        if dataset_type == "analytics":
            dataset = BIGQUERY_ANALYTICS_DATASET

        list_type_columns = [
            column_name
            for column_name in TABLES[table]["columns"]
            if "[]" in TABLES[table]["columns"][column_name]
        ]

        select_columns = ", ".join(
            [
                f"`{column_name}`"
                for column_name in TABLES[table]["columns"]
                if column_name not in list_type_columns
            ]
        )
        if table == "qpi_answers":
            filter_column_query = f"""
                SELECT {select_columns}
                FROM `{GCP_PROJECT}.{dataset}.{bigquery_table_name}_v3`
            """
        elif table == "firebase_events":
            filter_column_query = f"""SELECT {select_columns}
                FROM `{GCP_PROJECT}.{dataset}.{bigquery_table_name}`
                WHERE (event_name='ConsultOffer' OR event_name='HasAddedOfferToFavorites')
                AND (event_date > '{firebase_start_date}' AND event_date < '{firebase_end_date}')
                AND user_id is not null
                AND offer_id is not null
                AND offer_id != 'NaN'
                """
        else:
            filter_column_query = f"""
                SELECT {select_columns}
                FROM `{GCP_PROJECT}.{dataset}.{bigquery_table_name}`
            """

        filter_column_task = BigQueryExecuteQueryOperator(
            task_id=f"filter_column_{table}",
            sql=filter_column_query,
            use_legacy_sql=False,
            destination_dataset_table=f"{GCP_PROJECT}:{dataset}.temp_export_{table}",
            write_disposition="WRITE_TRUNCATE",
        )

        typed_columns = ", ".join(
            [
                f'"{col}" {TABLES[table]["columns"][col]}'
                for col in TABLES[table]["columns"]
                if "[]" not in TABLES[table]["columns"][col]
            ]
        )

        export_task = BigQueryToGCSOperator(
            task_id=f"export_{table}_to_gcs",
            source_project_dataset_table=f"{GCP_PROJECT}:{dataset}.temp_export_{table}",
            destination_cloud_storage_uris=[f"{BUCKET_PATH}/{table}-*.csv"],
            export_format="CSV",
            print_header=False,
        )

        delete_temp_table_task = BigQueryDeleteTableOperator(
            task_id=f"delete_temp_export_{table}_in_bigquery",
            deletion_dataset_table=f"{GCP_PROJECT}:{dataset}.temp_export_{table}",
            ignore_if_missing=True,
        )

        compose_files_task = PythonOperator(
            task_id=f"compose_files_{table}",
            python_callable=compose_gcs_files,
            op_kwargs={
                "bucket_name": DATA_GCS_BUCKET_NAME,
                "source_prefix": f"bigquery_exports/{table}-",
                "destination_blob_name": f"bigquery_exports/{table}.csv",
            },
            dag=dag,
        )

        drop_table_task = CloudSQLExecuteQueryOperator(
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            task_id=f"drop_table_public_{table}",
            sql=f"DROP TABLE IF EXISTS public.{table};",
            autocommit=True,
        )

        create_table_task = CloudSQLExecuteQueryOperator(
            task_id=f"create_table_public_{table}",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"CREATE TABLE IF NOT EXISTS public.{table} ({typed_columns});",
            autocommit=True,
        )

        start_drop_restore >> filter_column_task
        (
            filter_column_task
            >> export_task
            >> delete_temp_table_task
            >> compose_files_task
        )
        compose_files_task >> drop_table_task >> create_table_task >> end_data_prep

    def create_restore_task(table_name: str):
        import_body = {
            "importContext": {
                "fileType": "CSV",
                "csvImportOptions": {
                    "table": f"public.{table_name}",
                },
                "uri": f"{BUCKET_PATH}/{table_name}.csv",
                "database": RECOMMENDATION_SQL_BASE,
            }
        }

        sql_restore_task = CloudSQLImportInstanceOperator(
            task_id=f"cloud_sql_restore_table_{table_name}",
            project_id=GCP_PROJECT,
            body=import_body,
            instance=RECOMMENDATION_SQL_INSTANCE,
        )
        return sql_restore_task

    table_names = get_table_names()
    restore_tasks = []

    for index, table_name in enumerate(table_names):
        task = create_restore_task(table_name)
        restore_tasks.append(task)

        if index:
            restore_tasks[index - 1] >> restore_tasks[index]

    end_drop_restore = DummyOperator(task_id="end_drop_restore")

    end_data_prep >> restore_tasks[0]
    restore_tasks[-1] >> end_drop_restore

    recreate_indexes_query = """
        CREATE INDEX IF NOT EXISTS idx_stock_id                           ON public.stock                       USING btree (stock_id);
        CREATE INDEX IF NOT EXISTS idx_stock_offerid                      ON public.stock                       USING btree ("offer_id");
        CREATE INDEX IF NOT EXISTS idx_booking_stockid                    ON public.booking                     USING btree ("stock_id");
        CREATE INDEX IF NOT EXISTS idx_mediation_offerid                  ON public.mediation                   USING btree ("offerId");
        CREATE INDEX IF NOT EXISTS idx_offer_id                           ON public.offer                       USING btree (offer_id);
        CREATE INDEX IF NOT EXISTS idx_user_id                            ON public.enriched_user               USING btree (user_id);
        CREATE INDEX IF NOT EXISTS idx_offer_subcategoryid                ON public.offer                       USING btree ("offer_subcategoryId");
        CREATE INDEX IF NOT EXISTS idx_offer_venueid                      ON public.offer                       USING btree ("venue_id");
        CREATE INDEX IF NOT EXISTS idx_venue_id                           ON public.venue                       USING btree (venue_id);
        CREATE INDEX IF NOT EXISTS idx_venue_managingoffererid            ON public.venue                       USING btree ("venue_managing_offerer_id");
        CREATE INDEX IF NOT EXISTS idx_offerer_id                         ON public.offerer                     USING btree (offerer_id);
        CREATE INDEX IF NOT EXISTS idx_firebase_event_id                  ON public.firebase_events             USING btree (user_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_iris_venues_mv_unique       ON public.iris_venues_mv              USING btree (iris_id,venue_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_non_recommendable_id        ON public.non_recommendable_offers    USING btree (user_id,offer_id);
        CREATE INDEX IF NOT EXISTS idx_offer_recommendable_venue_id       ON public.recommendable_offers        USING btree (venue_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_recommendable_id      ON public.recommendable_offers        USING btree (offer_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_nb_bookings_unique          ON public.number_of_bookings_per_user USING btree ("user_id",bookings_count);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_nb_clicks_unique            ON public.number_of_clicks_per_user   USING btree ("user_id",clicks_count);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_nb_favorites_unique         ON public.number_of_favorites_per_user USING btree ("user_id",favorites_count);
        CREATE INDEX IF NOT EXISTS qpi_answers_user_id                    ON public.qpi_answers                 USING btree (user_id);
        CREATE INDEX IF NOT EXISTS trained_users_mf_reco_user_id          ON public.trained_users_mf_reco       USING btree (user_id);
    """

    recreate_indexes_task = CloudSQLExecuteQueryOperator(
        task_id="recreate_indexes",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql=recreate_indexes_query,
        autocommit=True,
    )

    views_to_refresh = [
        "recommendable_offers",
        "recommendable_offers_eac_15",
        "recommendable_offers_eac_16_17",
        "non_recommendable_offers",
        "iris_venues_mv",
        "number_of_bookings_per_user",
        "number_of_clicks_per_user",
        "number_of_favorites_per_user",
    ]

    refresh_materialized_view_tasks = []
    for materialized_view in views_to_refresh:
        refresh_materialized_view_task = CloudSQLExecuteQueryOperator(
            task_id=f"refresh_{materialized_view}",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"REFRESH MATERIALIZED VIEW CONCURRENTLY {materialized_view};",
            autocommit=True,
        )
        refresh_materialized_view_tasks.append(refresh_materialized_view_task)

    end = DummyOperator(task_id="end")

    start >> start_drop_restore
    end_drop_restore >> recreate_indexes_task >> refresh_materialized_view_tasks >> end
