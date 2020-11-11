import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator, CloudSqlInstanceImportOperator

GCP_PROJECT_ID = "pass-culture-app-projet-test"
BUCKET_PATH = 'gs://pass-culture-data'
RECOMMENDATION_SQL_DUMP = 'dump_staging_for_recommendations_09_11_20.gz'
RECOMMENDATION_SQL_INSTANCE = 'pcdata-poc-csql-recommendation'
RECOMMENDATION_SQL_BASE = 'pcdata-poc-csql-recommendation'
TYPE = 'postgres'
LOCATION = "europe-west1-d"

RECOMMENDATION_SQL_USER = os.getenv('RECOMMENDATION_SQL_USER')
RECOMMENDATION_SQL_PASSWORD = os.getenv('RECOMMENDATION_SQL_PASSWORD')
RECOMMENDATION_SQL_PUBLIC_IP = os.getenv('RECOMMENDATION_SQL_PUBLIC_IP')
RECOMMENDATION_SQL_PUBLIC_PORT = os.getenv('RECOMMENDATION_SQL_PUBLIC_PORT')

TABLES = ['booking', 'offer', 'iris_venues', 'stock', 'mediation', 'venue', 'offerer']
VIEWS = ['recommendable_offers']

os.environ['AIRFLOW_CONN_PROXY_POSTGRES_TCP'] = \
    f"gcpcloudsql://{RECOMMENDATION_SQL_USER}:{RECOMMENDATION_SQL_PASSWORD}@{RECOMMENDATION_SQL_PUBLIC_IP}:{RECOMMENDATION_SQL_PUBLIC_PORT}/{RECOMMENDATION_SQL_BASE}?" \
    f"database_type={TYPE}&" \
    f"project_id={GCP_PROJECT_ID}&" \
    f"location={LOCATION}&" \
    f"instance={RECOMMENDATION_SQL_INSTANCE}&" \
    f"use_proxy=True&" \
    f"sql_proxy_use_tcp=True"

default_args = {
    'start_date': datetime(2020, 11, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
    'recommendation_cloud_sql_v5',
    default_args=default_args,
    description='Restore postgres dumps to Cloud SQL',
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=90)
) as dag:

    start = DummyOperator(task_id='start')

    drop_table_tasks = []

    for table in TABLES:
        task = CloudSqlQueryOperator(
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            task_id=f"drop_table_public_{table}",
            sql=f"DROP TABLE IF EXISTS public.{table}",
            autocommit=True
        )
        drop_table_tasks.append(task)

    import_body = {
        "importContext": {
            "fileType": "sql",
            "uri": f"{BUCKET_PATH}/{RECOMMENDATION_SQL_DUMP}",
            "database": RECOMMENDATION_SQL_BASE
        }
    }

    sql_restore_task = CloudSqlInstanceImportOperator(
        project_id=GCP_PROJECT_ID,
        body=import_body,
        instance=RECOMMENDATION_SQL_INSTANCE,
        task_id='sql_restore_task'
    )

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
        autocommit=True
    )

    refresh_materialized_view_tasks = []

    for view in VIEWS:
        task = CloudSqlQueryOperator(
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            task_id=f"refresh_materialized_view_{view}",
            sql=f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}",
            autocommit=True
        )
        refresh_materialized_view_tasks.append(task)

    end = DummyOperator(task_id='end')

    start >> drop_table_tasks >> sql_restore_task >> recreate_indexes_task >> refresh_materialized_view_tasks >> end
