import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import (
    CloudSqlQueryOperator,
    CloudSqlInstanceImportOperator,
)
from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = "/home/airflow/gcs/dags/credentials.json"
TABLES_DATA_PATH = "/home/airflow/gcs/dags/tables.csv"

GCP_PROJECT_ID = "pass-culture-app-projet-test"
BIGQUERY_DATASET = "poc_data_federated_query"
BIGQUERY_TEMPORARY_DATASET = "temporary"

BUCKET_PATH = "gs://pass-culture-data/bigquery_exports"
RECOMMENDATION_SQL_INSTANCE = "pcdata-poc-csql-recommendation"
RECOMMENDATION_SQL_BASE = "pcdata-poc-csql-recommendation"
TYPE = "postgres"
LOCATION = "europe-west1"

RECOMMENDATION_SQL_USER = os.environ.get("RECOMMENDATION_SQL_USER")
RECOMMENDATION_SQL_PASSWORD = os.environ.get("RECOMMENDATION_SQL_PASSWORD")
RECOMMENDATION_SQL_PUBLIC_IP = os.environ.get("RECOMMENDATION_SQL_PUBLIC_IP")
RECOMMENDATION_SQL_PUBLIC_PORT = os.environ.get("RECOMMENDATION_SQL_PUBLIC_PORT")


TYPE_MAPPING = {
    "DATETIME": "timestamp without time zone",
    "STRING": "text",
    "BOOL": "boolean",
    "INT64": "bigint",
    "NUMERIC": "numeric",
    "BYTES": "bytea",
}

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
    "start_date": datetime(2020, 11, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

TABLES = {}
tables = pd.read_csv(TABLES_DATA_PATH)
for table in set(tables["table_name"].values):

    table_data = tables.loc[lambda df: df.table_name == table]
    TABLES[table] = {
        column_name: data_type
        for column_name, data_type in zip(
            list(table_data.column_name.values),
            list(table_data.data_type.values),
        )
    }


def map_bigquery_to_postgres_types(col_type):
    if col_type in TYPE_MAPPING:
        return TYPE_MAPPING[col_type]
    return col_type.lower()


with DAG(
    "recommendation_cloud_sql_v32",
    default_args=default_args,
    description="Restore postgres dumps to Cloud SQL",
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=90),
) as dag:

    start = DummyOperator(task_id="start")

    start_drop_restore = DummyOperator(task_id="start_drop_restore")
    end_drop_restore = DummyOperator(task_id="end_drop_restore")

    for table in TABLES:

        drop_table_task = CloudSqlQueryOperator(
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            task_id=f"drop_table_public_{table}",
            sql=f"DROP TABLE IF EXISTS public.{table};",
            autocommit=True,
        )

        table_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BIGQUERY_DATASET).table(
            table
        )
        schema = client.get_table(table_ref).schema
        drop_columns = ", ".join([col.name for col in schema if col.mode == "REPEATED"])
        drop_columns_query = f"except ({drop_columns})" if len(drop_columns) > 0 else ""

        drop_column_task = BigQueryOperator(
            task_id=f"drop_column_{table}",
            bql=f"""
                SELECT * {drop_columns_query}
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table}` 
            """,
            use_legacy_sql=False,
            destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_TEMPORARY_DATASET}.{table}",
            write_disposition="WRITE_TRUNCATE",
        )

        typed_columns = {
            col.name: TABLES[table].get(
                col.name, map_bigquery_to_postgres_types(col.field_type)
            )
            for col in schema
            if col.mode != "REPEATED"
        }
        typed_columns_string = ", ".join(
            [f'"{col}" {typed_columns[col]}' for col in typed_columns]
        )

        export_task = BigQueryToCloudStorageOperator(
            task_id=f"export_{table}_to_gcs",
            source_project_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_TEMPORARY_DATASET}.{table}",
            destination_cloud_storage_uris=[f"{BUCKET_PATH}/{table}"],
            export_format="CSV",
            print_header=False,
        )

        create_table_task = CloudSqlQueryOperator(
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            task_id=f"create_table_public_{table}",
            sql=f"CREATE TABLE IF NOT EXISTS public.{table} ({typed_columns_string});",
            autocommit=True,
        )

        import_body = {
            "importContext": {
                "fileType": "CSV",
                "csvImportOptions": {
                    "table": f"public.{table}",
                },
                "uri": f"{BUCKET_PATH}/{table}",
                "database": RECOMMENDATION_SQL_BASE,
            }
        }

        sql_restore_task = CloudSqlInstanceImportOperator(
            project_id=GCP_PROJECT_ID,
            body=import_body,
            instance=RECOMMENDATION_SQL_INSTANCE,
            task_id=f"cloud_sql_restore_table_{table}",
        )

        start_drop_restore >> drop_table_task >> drop_column_task >> export_task >> create_table_task >> sql_restore_task >> end_drop_restore

    recreate_indexes_query = """
        CREATE INDEX IF NOT EXISTS idx_stock_id ON public.stock USING btree (id);
        CREATE INDEX IF NOT EXISTS idx_stock_offerid ON public.stock USING btree ("offerId");
        CREATE INDEX IF NOT EXISTS idx_booking_stockid ON public.booking USING btree ("stockId");
        CREATE INDEX IF NOT EXISTS idx_mediation_offerid ON public.mediation USING btree ("offerId");
        CREATE INDEX IF NOT EXISTS idx_offer_id ON public.offer USING btree (id);
        CREATE INDEX IF NOT EXISTS idx_offer_type ON public.offer USING btree (type);
        CREATE INDEX IF NOT EXISTS idx_offer_venueid ON public.offer USING btree ("venueId");
        CREATE INDEX IF NOT EXISTS idx_venue_id ON public.venue USING btree (id);
        CREATE INDEX IF NOT EXISTS idx_venue_managingoffererid ON public.venue USING btree ("managingOffererId");
        CREATE INDEX IF NOT EXISTS idx_offerer_id ON public.offerer USING btree (id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_recommendable_id ON recommendable_offers USING btree (id);
    """

    recreate_indexes_task = CloudSqlQueryOperator(
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        task_id="recreate_indexes",
        sql=recreate_indexes_query,
        autocommit=True,
    )

    refresh_recommendable_offers = CloudSqlQueryOperator(
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        task_id="refresh_recommendable_offers",
        sql="REFRESH MATERIALIZED VIEW CONCURRENTLY recommendable_offers;",
        autocommit=True,
    )

    refresh_non_recommendable_offers = CloudSqlQueryOperator(
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        task_id="refresh_non_recommendable_offers",
        sql="REFRESH MATERIALIZED VIEW non_recommendable_offers;",
        autocommit=True,
    )

    refresh_materialized_views_tasks = [
        refresh_recommendable_offers,
        refresh_non_recommendable_offers,
    ]

    end = DummyOperator(task_id="end")

    start >> start_drop_restore
    end_drop_restore >> recreate_indexes_task >> refresh_materialized_views_tasks >> end
