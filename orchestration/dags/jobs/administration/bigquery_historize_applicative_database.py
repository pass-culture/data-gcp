import datetime

from common.callback import on_failure_base_callback
from common.config import (
    BIGQUERY_INT_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DE_BIGQUERY_DATA_ARCHIVE_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
    get_tables_config_dict,
)
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.utils.task_group import TaskGroup

GCE_INSTANCE = f"bq-historize-applicative-database-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/export_applicative"

SNAPSHOT_MODELS_PATH = "data_gcp_dbt/models/intermediate/raw"
SNAPSHOT_TABLES = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + SNAPSHOT_MODELS_PATH,
    BQ_DATASET=BIGQUERY_INT_RAW_DATASET,
    is_source=True,
    dbt_alias=True,
)

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 6,
    "on_failure_callback": on_failure_base_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}


dag_id = "bigquery_historize_applicative_database"
dag = DAG(
    dag_id,
    default_args=default_dag_args,
    dagrun_timeout=datetime.timedelta(minutes=480),
    description="historize applicative database current state to gcs bucket",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[dag_id][ENV_SHORT_NAME]),
    catchup=False,
    tags=[DAG_TAGS.DE.value],
)

start = DummyOperator(task_id="start", dag=dag)

with TaskGroup(group_id="snapshots_to_gcs", dag=dag) as to_gcs:
    for table_name, bq_config in SNAPSHOT_TABLES.items():
        alias = bq_config["table_alias"]
        dataset = bq_config["source_dataset"]
        _now = datetime.datetime.now()
        historization_date = _now - datetime.timedelta(days=1)
        yyyymmdd = historization_date.strftime("%Y%m%d")

        waiting_task = delayed_waiting_operator(
            dag,
            external_dag_id="dbt_run_dag",
            external_task_id=f"data_transformation.{table_name}",
        )
        export_bigquery_to_gcs = BigQueryToGCSOperator(
            task_id=f"export_bq_to_gcs_{table_name}",
            source_project_dataset_table=f"{GCP_PROJECT_ID}.{dataset}.{alias}",
            destination_cloud_storage_uris=[
                f"gs://{DE_BIGQUERY_DATA_ARCHIVE_NAME}/applicative/{alias}/{yyyymmdd}/*.parquet"
            ],
            export_format="PARQUET",
            field_delimiter=",",
            print_header=True,
            dag=dag,
        )
        waiting_task >> export_bigquery_to_gcs

end = DummyOperator(task_id="end", dag=dag)

start >> to_gcs >> end
