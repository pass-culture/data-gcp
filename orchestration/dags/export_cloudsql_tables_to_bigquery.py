import datetime
import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.access_gcp_secrets import access_secret_data
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
)

AB_TESTING_TABLE = os.environ.get("TABLE_AB_TESTING", "abc_testing_20220322_v1v2")
yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(
    "%Y-%m-%d"
) + " 00:00:00"
TABLES = {
    f"{AB_TESTING_TABLE}": {
        "query": None,
        "write_disposition": "WRITE_TRUNCATE",
    },
    "past_recommended_offers": {
        "query": f"SELECT * FROM public.past_recommended_offers where date <= '{yesterday}'",
        "write_disposition": "WRITE_APPEND",
    },
}
GCP_PROJECT = os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("REGION")

RECOMMENDATION_SQL_INSTANCE = os.environ.get("RECOMMENDATION_SQL_INSTANCE")
RECOMMENDATION_SQL_BASE = os.environ.get("RECOMMENDATION_SQL_BASE")
CONNECTION_ID = os.environ.get("BIGQUERY_CONNECTION_RECOMMENDATION")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET")

# Recreate proprely the connection url
database_url = access_secret_data(
    GCP_PROJECT, f"{RECOMMENDATION_SQL_BASE}-database-url"
)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    database_url.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT}&location={LOCATION}&instance={RECOMMENDATION_SQL_INSTANCE}&use_proxy=True&sql_proxy_use_tcp=True"
)

default_dag_args = {
    "start_date": datetime.datetime(2021, 2, 2),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "export_cloudsql_tables_to_bigquery_v1",
    default_args=default_dag_args,
    description="Export tables from recommendation CloudSQL to BigQuery",
    schedule_interval="0 3 * * *",
    on_failure_callback=task_fail_slack_alert,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

export_table_tasks = []
for table in TABLES:
    query = TABLES[table]["query"]
    if query is None:
        query = f"SELECT * FROM public.{table}"
    task = BigQueryOperator(
        task_id=f"import_{table}",
        sql=f'SELECT * FROM EXTERNAL_QUERY("{CONNECTION_ID}", "{query}");',
        write_disposition=TABLES[table]["write_disposition"],
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.{table}",
        dag=dag,
    )
    export_table_tasks.append(task)

delete_rows_task = drop_table_task = CloudSqlQueryOperator(
    task_id="drop_yesterday_rows_past_recommended_offers",
    gcp_cloudsql_conn_id="proxy_postgres_tcp",
    sql=f"DELETE FROM public.past_recommended_offers where date <= '{yesterday}'",
    autocommit=True,
    dag=dag,
)

copy_to_clean_past_recommended_offers = BigQueryOperator(
    task_id="copy_to_clean_past_recommended_offers",
    sql=f"SELECT * FROM {BIGQUERY_RAW_DATASET}.past_recommended_offers",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.past_recommended_offers",
    dag=dag,
)

copy_to_analytics_past_recommended_offers = BigQueryOperator(
    task_id="copy_to_analytics_past_recommended_offers",
    sql=f"SELECT * FROM {BIGQUERY_CLEAN_DATASET}.past_recommended_offers",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.past_recommended_offers",
    dag=dag,
)

copy_to_clean_ab_testing = BigQueryOperator(
    task_id="copy_to_clean_ab_testing",
    sql=f"SELECT * FROM {BIGQUERY_RAW_DATASET}.{AB_TESTING_TABLE}",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{AB_TESTING_TABLE}",
    dag=dag,
)

copy_to_analytics_ab_testing = BigQueryOperator(
    task_id="copy_to_analytics_ab_testing",
    sql=f"SELECT * FROM {BIGQUERY_CLEAN_DATASET}.{AB_TESTING_TABLE}",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{AB_TESTING_TABLE}",
    dag=dag,
)


end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> export_table_tasks
    >> delete_rows_task
    >> copy_to_clean_past_recommended_offers
    >> copy_to_analytics_past_recommended_offers
    >> end
)
(delete_rows_task >> copy_to_clean_ab_testing >> copy_to_analytics_ab_testing >> end)
