import os
from datetime import timedelta

import airflow
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

RECOMMENDATION_SQL_USER = os.getenv('SQL_USER')
RECOMMENDATION_SQL_PASSWORD = os.getenv('SQL_PASSWORD')
RECOMMENDATION_SQL_PUBLIC_IP = os.getenv('SQL_PUBLIC_IP')
RECOMMENDATION_SQL_PUBLIC_PORT = os.getenv('SQL_PUBLIC_PORT')

os.environ['AIRFLOW_CONN_PROXY_POSTGRES_TCP'] = \
    f"gcpcloudsql://{RECOMMENDATION_SQL_USER}:{RECOMMENDATION_SQL_PASSWORD}@{RECOMMENDATION_SQL_PUBLIC_IP}:{RECOMMENDATION_SQL_PUBLIC_PORT}/{RECOMMENDATION_SQL_BASE}?" \
    f"database_type={TYPE}&" \
    f"project_id={GCP_PROJECT_ID}&" \
    f"location={LOCATION}&" \
    f"instance={RECOMMENDATION_SQL_INSTANCE}&" \
    f"use_proxy=True&" \
    f"sql_proxy_use_tcp=True"

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'restore_cloud_sql_from_gcs',
    default_args=default_args,
    description='Restore postgres dumps to Cloud SQL',
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=90)
)

start = DummyOperator(task_id='start', dag=dag)

tasks = []

for table in ['booking', 'offer', 'iris_venues', 'stock', 'mediation', 'venue', 'offerer']:
    task = CloudSqlQueryOperator(
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        task_id=f"drop_table_public_{table}",
        sql=f"DROP TABLE IF EXISTS public.{table}",
        autocommit=True,
        dag=dag
    )
    tasks.append(task)

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
    task_id='sql_restore_task',
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> tasks >> sql_restore_task >> end
