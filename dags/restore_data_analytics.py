import datetime

from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.operators import bash_operator
from airflow.operators.dummy_operator import DummyOperator


BIGQUERY_AIRFLOW_DATASET = 'pcdata_poc_bqds_airflow'
DATA_ANALYTICS_TABLES = [
    "user", "provider", "offerer", "bank_information", "booking", "payment", "venue", "user_offerer", "offer", "stock",
    "favorite", "venue_type", "venue_label"
]
GCP_PROJECT_ID = "pass-culture-app-projet-test"  # @TODO: use config variables
GCP_REGION = "europe-west1"  # @TODO: use config variables
CLOUDSQL_DATABASE = "cloud_SQL_dump-prod-8-10-2020"  # @TODO: use config variables

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time()
)

default_dag_args = {
    "start_date": yesterday,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID
}

dag = DAG(
    "restore_data_analytics",
    default_args=default_dag_args,
    description="Restore postgres dumps to Cloud SQL",
    schedule_interval="@daily",
    dagrun_timeout=datetime.timedelta(minutes=90)
)

# DAG start task
start = DummyOperator(task_id="start", dag=dag)

# Create BigQuery output dataset (if not exists)
make_bq_dataset_task = bash_operator.BashOperator(
    task_id='make_bq_dataset',
    # Executing 'bq' command requires Google Cloud SDK which comes preinstalled in Cloud Composer.
    bash_command=f'bq ls {BIGQUERY_AIRFLOW_DATASET} || bq mk --dataset --location {GCP_REGION} {BIGQUERY_AIRFLOW_DATASET}',
    dag=dag
)

# Import useful tables from CloudSQL
import_tables_tasks = []
for table in DATA_ANALYTICS_TABLES[-1:]:  # @TODO: only for test sake
    task = bigquery_operator.BigQueryOperator(
        task_id=f"import_{table}",
        sql=f"SELECT * FROM EXTERNAL_QUERY('{GCP_PROJECT_ID}.{GCP_REGION}.{CLOUDSQL_DATABASE}', 'SELECT * FROM {table}');",  # @TODO: factorize with import_tables.py
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_AIRFLOW_DATASET}.{table}",
        dag=dag
    )
    import_tables_tasks.append(task)

# Anonymize data
anonymization_task = DummyOperator(task_id="anonymization", dag=dag)

# Create enriched data tables
create_enriched_data_tasks = DummyOperator(task_id="create_enriched_data", dag=dag)

# DAG end task
end = DummyOperator(task_id='end', dag=dag)

# Define DAG dependencies
start >> make_bq_dataset_task >> import_tables_tasks >> anonymization_task >> create_enriched_data_tasks >> end
