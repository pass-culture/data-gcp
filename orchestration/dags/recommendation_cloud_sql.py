import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcp_sql_operator import (
    CloudSqlQueryOperator,
    CloudSqlInstanceImportOperator,
)

from dependencies.slack_alert import task_fail_slack_alert
from dependencies.compose_gcs_files import compose_gcs_files

TABLES_DATA_PATH = f"{os.environ.get('DAG_FOLDER')}/tables.csv"
GCP_PROJECT_ID = "pass-culture-app-projet-test"
BIGQUERY_DATASET = "dump_staging_for_reco"
BIGQUERY_TEMPORARY_DATASET = "temporary"

BUCKET_NAME = "pass-culture-data"
BUCKET_PATH = "gs://pass-culture-data/bigquery_exports"
RECOMMENDATION_SQL_INSTANCE = "pcdata-poc-csql-recommendation"
RECOMMENDATION_SQL_BASE = "pcdata-poc-csql-recommendation"
TYPE = "postgres"
LOCATION = "europe-west1"

RECOMMENDATION_SQL_USER = os.environ.get("RECOMMENDATION_SQL_USER")
RECOMMENDATION_SQL_PASSWORD = os.environ.get("RECOMMENDATION_SQL_PASSWORD")
RECOMMENDATION_SQL_PUBLIC_IP = os.environ.get("RECOMMENDATION_SQL_PUBLIC_IP")
RECOMMENDATION_SQL_PUBLIC_PORT = os.environ.get("RECOMMENDATION_SQL_PUBLIC_PORT")

os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    f"gcpcloudsql://{RECOMMENDATION_SQL_USER}:{RECOMMENDATION_SQL_PASSWORD}@{RECOMMENDATION_SQL_PUBLIC_IP}:{RECOMMENDATION_SQL_PUBLIC_PORT}/{RECOMMENDATION_SQL_BASE}?"
    f"database_type={TYPE}&"
    f"project_id={GCP_PROJECT_ID}&"
    f"location={LOCATION}&"
    f"instance={RECOMMENDATION_SQL_INSTANCE}&"
    f"use_proxy=True&"
    f"sql_proxy_use_tcp=True"
)

default_args = {
    "on_failure_callback": task_fail_slack_alert,
    "start_date": datetime(2020, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def get_table_data():
    data = {}
    tables = pd.read_csv(TABLES_DATA_PATH)
    for table_name in set(tables["table_name"].values):

        table_data = tables.loc[lambda df: df.table_name == table_name]
        data[table_name] = {
            column_name: data_type
            for column_name, data_type in zip(
                list(table_data.column_name.values),
                list(table_data.data_type.values),
            )
        }
    return data


TABLES = get_table_data()


def get_table_names():
    tables = pd.read_csv(TABLES_DATA_PATH)
    table_names = tables.table_name.unique()
    return table_names


with DAG(
    "recommendation_cloud_sql_v42",
    default_args=default_args,
    description="Export bigQuery tables to GCS to dump and restore Cloud SQL tables",
    schedule_interval="@daily",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    start_drop_restore = DummyOperator(task_id="start_drop_restore")
    end_data_prep = DummyOperator(task_id="end_data_prep")

    for table in TABLES:

        drop_table_task = CloudSqlQueryOperator(
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            task_id=f"drop_table_public_{table}",
            sql=f"DROP TABLE IF EXISTS public.{table};",
            autocommit=True,
        )

        select_columns = ", ".join(
            [
                column_name
                for column_name in TABLES[table]
                if "[]" not in TABLES[table][column_name]
            ]
        )

        filter_column_task = BigQueryOperator(
            task_id=f"filter_column_{table}",
            sql=f"""
                SELECT {select_columns}
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table}` 
            """,
            use_legacy_sql=False,
            destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_TEMPORARY_DATASET}.{table}",
            write_disposition="WRITE_TRUNCATE",
        )

        typed_columns = ", ".join(
            [
                f'"{col}" {TABLES[table][col]}'
                for col in TABLES[table]
                if "[]" not in TABLES[table][col]
            ]
        )

        export_task = BigQueryToCloudStorageOperator(
            task_id=f"export_{table}_to_gcs",
            source_project_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_TEMPORARY_DATASET}.{table}",
            destination_cloud_storage_uris=[f"{BUCKET_PATH}/{table}-*.csv"],
            export_format="CSV",
            print_header=False,
        )

        compose_files_task = PythonOperator(
            task_id=f"compose_files_{table}",
            python_callable=compose_gcs_files,
            op_kwargs={
                "bucket_name": BUCKET_NAME,
                "source_prefix": f"bigquery_exports/{table}-",
                "destination_blob_name": f"bigquery_exports/{table}.csv",
            },
            dag=dag,
        )

        create_table_task = CloudSqlQueryOperator(
            task_id=f"create_table_public_{table}",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"CREATE TABLE IF NOT EXISTS public.{table} ({typed_columns});",
            autocommit=True,
        )

        start_drop_restore >> drop_table_task >> filter_column_task
        filter_column_task >> export_task >> compose_files_task
        compose_files_task >> create_table_task >> end_data_prep

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

        sql_restore_task = CloudSqlInstanceImportOperator(
            task_id=f"cloud_sql_restore_table_{table_name}",
            project_id=GCP_PROJECT_ID,
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
        CREATE INDEX IF NOT EXISTS idx_stock_id                      ON public.stock                    USING btree (id);
        CREATE INDEX IF NOT EXISTS idx_stock_offerid                 ON public.stock                    USING btree ("offerId");
        CREATE INDEX IF NOT EXISTS idx_booking_stockid               ON public.booking                  USING btree ("stockId");
        CREATE INDEX IF NOT EXISTS idx_mediation_offerid             ON public.mediation                USING btree ("offerId");
        CREATE INDEX IF NOT EXISTS idx_offer_id                      ON public.offer                    USING btree (id);
        CREATE INDEX IF NOT EXISTS idx_offer_type                    ON public.offer                    USING btree (type);
        CREATE INDEX IF NOT EXISTS idx_offer_venueid                 ON public.offer                    USING btree ("venueId");
        CREATE INDEX IF NOT EXISTS idx_venue_id                      ON public.venue                    USING btree (id);
        CREATE INDEX IF NOT EXISTS idx_venue_managingoffererid       ON public.venue                    USING btree ("managingOffererId");
        CREATE INDEX IF NOT EXISTS idx_offerer_id                    ON public.offerer                  USING btree (id);
        CREATE INDEX IF NOT EXISTS idx_iris_venues_mv_irisid         ON public.iris_venues_mv           USING btree (iris_id);
        CREATE INDEX IF NOT EXISTS idx_non_recommendable_userid      ON public.non_recommendable_offers USING btree (user_id);
        CREATE INDEX IF NOT EXISTS idx_offer_recommendable_venue_id  ON public.recommendable_offers     USING btree (venue_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_recommendable_id ON public.recommendable_offers     USING btree (id);
    """

    recreate_indexes_task = CloudSqlQueryOperator(
        task_id="recreate_indexes",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql=recreate_indexes_query,
        autocommit=True,
    )

    refresh_recommendable_offers = CloudSqlQueryOperator(
        task_id="refresh_recommendable_offers",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql="REFRESH MATERIALIZED VIEW CONCURRENTLY recommendable_offers;",
        autocommit=True,
    )

    refresh_non_recommendable_offers = CloudSqlQueryOperator(
        task_id="refresh_non_recommendable_offers",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql="REFRESH MATERIALIZED VIEW non_recommendable_offers;",
        autocommit=True,
    )

    refresh_iris_venues_mv = CloudSqlQueryOperator(
        task_id="refresh_iris_venues_mv",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql="REFRESH MATERIALIZED VIEW iris_venues_mv;",
        autocommit=True,
    )

    refresh_materialized_views_tasks = [
        refresh_recommendable_offers,
        refresh_non_recommendable_offers,
        refresh_iris_venues_mv,
    ]

    end = DummyOperator(task_id="end")

    start >> start_drop_restore
    end_drop_restore >> recreate_indexes_task >> refresh_materialized_views_tasks >> end
