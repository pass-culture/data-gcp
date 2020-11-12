import datetime

from airflow import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.operators import bash_operator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.data_analytics.config import (
    GCP_PROJECT_ID, GCP_REGION, BIGQUERY_AIRFLOW_DATASET
)
from dependencies.data_analytics.import_tables import define_import_query
from dependencies.data_analytics.anonymization import define_anonymization_query
from dependencies.data_analytics.enriched_data.offer import define_enriched_offer_data_full_query
from dependencies.data_analytics.enriched_data.offerer import define_enriched_offerer_data_full_query
from dependencies.data_analytics.enriched_data.user import define_enriched_user_data_full_query
from dependencies.data_analytics.enriched_data.venue import define_enriched_venue_data_full_query


data_analytics_tables = [
    "user", "provider", "offerer", "bank_information", "booking", "payment", "venue", "user_offerer", "offer", "stock",
    "favorite", "venue_type", "venue_label"
]

default_dag_args = {
    "start_date": datetime.datetime(2020, 11, 11),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID
}

dag = DAG(
    "restore_data_analytics_v1",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for Data Analytics",
    schedule_interval="@daily",
    catchup=False,
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
for table in data_analytics_tables:
    task = bigquery_operator.BigQueryOperator(
        task_id=f"import_{table}",
        sql=define_import_query(table=table),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_AIRFLOW_DATASET}.{table}",
        dag=dag
    )
    import_tables_tasks.append(task)

# Anonymize data
anonymization_task = bigquery_operator.BigQueryOperator(
    task_id="anonymization",
    sql=define_anonymization_query(dataset=BIGQUERY_AIRFLOW_DATASET),
    use_legacy_sql=False,
    dag=dag
)

# Create enriched data tables
create_enriched_offer_data_task = bigquery_operator.BigQueryOperator(
    task_id="create_enriched_offer_data",
    sql=define_enriched_offer_data_full_query(dataset=BIGQUERY_AIRFLOW_DATASET),
    use_legacy_sql=False,
    dag=dag
)
create_enriched_offerer_data_task = bigquery_operator.BigQueryOperator(
    task_id="create_enriched_offerer_data",
    sql=define_enriched_offerer_data_full_query(dataset=BIGQUERY_AIRFLOW_DATASET),
    use_legacy_sql=False,
    dag=dag
)
create_enriched_user_data_task = bigquery_operator.BigQueryOperator(
    task_id="create_enriched_user_data",
    sql=define_enriched_user_data_full_query(dataset=BIGQUERY_AIRFLOW_DATASET),
    use_legacy_sql=False,
    dag=dag
)
create_enriched_venue_data_task = bigquery_operator.BigQueryOperator(
    task_id="create_enriched_venue_data",
    sql=define_enriched_venue_data_full_query(dataset=BIGQUERY_AIRFLOW_DATASET),
    use_legacy_sql=False,
    dag=dag
)
create_enriched_data_tasks = [
    create_enriched_offer_data_task,
    create_enriched_offerer_data_task,
    create_enriched_user_data_task,
    create_enriched_venue_data_task,
]

# DAG end task
end = DummyOperator(task_id='end', dag=dag)

# Define DAG dependencies
start >> make_bq_dataset_task >> import_tables_tasks >> anonymization_task >> create_enriched_data_tasks >> end
