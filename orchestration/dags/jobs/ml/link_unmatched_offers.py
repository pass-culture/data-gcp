import os
from datetime import datetime, timedelta

from common import macros
from common.config import (
    BIGQUERY_SANDBOX_DATASET,
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MLFLOW_BUCKET_NAME,
)
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule
from jobs.ml.constants import IMPORT_LINKAGE_SQL_PATH

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
GCS_FOLDER_PATH = f"linkage_item_{ENV_SHORT_NAME}/linkage_{DATE}"
DAG_CONFIG = {
    "GCS_FOLDER_PATH": GCS_FOLDER_PATH,
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/{GCS_FOLDER_PATH}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/item_linkage/",
    "EXPERIMENT_NAME": f"linkage_semantic_vector_v1.0_{ENV_SHORT_NAME}",
    "REDUCTION": "true",
    "BATCH_SIZE": 100000,
    "LINKAGE_ITEM_SOURCES_DATA_REQUEST": "linkage_item_sources_data.sql",
    "LINKAGE_ITEM_CANDIDATES_DATA_REQUEST": "linkage_item_candidates_unmatched_data.sql",
    "INPUT_SOURCES_TABLE": f"{DATE}_input_sources_table",
    "INPUT_CANDIDATES_TABLE": f"{DATE}_item_candidates_data",
    "LINKED_ITEMS_TABLE": "linked_items",
    "INPUT_SOURCES_DIR": "item_sources_data",
    "INPUT_CANDIDATES_DIR": "item_candidates_data",
    "LINKAGE_CANDIDATES_FILENAME": "linkage_candidates_items.parquet",
    "LINKED_ITEMS_FILENAME": "linkage_unmatched_offers.parquet",
}
GCE_PARAMS = {
    "instance_name": f"linkage-item-unmatched-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-8",
        "prod": "n1-standard-32",
    },
}

DEFAULT_ARGS = {
    "start_date": datetime(2022, 11, 30),
    # "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

SCHEDULE_DICT = {"prod": "0 4 * * 3", "stg": "0 6 * * 3", "dev": "0 6 * * 3"}

with DAG(
    "link_unmatched_offers",
    default_args=DEFAULT_ARGS,
    description="Process to link unmatched items using semantic vectors.",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[ENV_SHORT_NAME]),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=GCE_PARAMS["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=GCE_PARAMS["instance_name"],
            type="string",
        ),
        "reduction": Param(
            default=DAG_CONFIG["REDUCTION"],
            type="string",
        ),
        "batch_size": Param(
            default=DAG_CONFIG["BATCH_SIZE"],
            type="integer",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    import_data_tasks = []
    import_sources = BigQueryExecuteQueryOperator(
        task_id="import_sources",
        sql=(
            IMPORT_LINKAGE_SQL_PATH / DAG_CONFIG["LINKAGE_ITEM_SOURCES_DATA_REQUEST"]
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DAG_CONFIG['INPUT_SOURCES_TABLE']}",
    )
    import_data_tasks.append(import_sources)

    import_candidates = BigQueryExecuteQueryOperator(
        task_id="import_candidates",
        sql=(
            IMPORT_LINKAGE_SQL_PATH / DAG_CONFIG["LINKAGE_ITEM_CANDIDATES_DATA_REQUEST"]
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DAG_CONFIG['INPUT_CANDIDATES_TABLE']}",
    )
    import_data_tasks.append(import_candidates)
    end_imports = DummyOperator(task_id="end_imports", dag=dag)
    export_to_bq_tasks = []
    export_sources_bq = BigQueryInsertJobOperator(
        task_id="export_sources_bq",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": f"{DAG_CONFIG['INPUT_CANDIDATES_TABLE']}",
                },
                "compression": None,
                "destinationFormat": "PARQUET",
                "destinationUris": os.path.join(
                    DAG_CONFIG["STORAGE_PATH"],
                    DAG_CONFIG["INPUT_SOURCES_DIR"],
                    "data-*.parquet",
                ),
            }
        },
    )

    export_to_bq_tasks.append(export_sources_bq)

    export_candidates_bq = BigQueryInsertJobOperator(
        task_id="export_candidates_bq",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": DAG_CONFIG["INPUT_CANDIDATES_TABLE"],
                },
                "compression": None,
                "destinationFormat": "PARQUET",
                "destinationUris": os.path.join(
                    DAG_CONFIG["STORAGE_PATH"],
                    DAG_CONFIG["INPUT_CANDIDATES_DIR"],
                    "data-*.parquet",
                ),
            }
        },
    )
    export_to_bq_tasks.append(export_candidates_bq)
    end_exports = DummyOperator(task_id="end_exports", dag=dag)
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "long_ml"},
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name="{{ params.instance_name }}",
        python_version="3.10",
        command="{{ params.branch }}",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=DAG_CONFIG["BASE_DIR"],
        command="pip install -r requirements.txt --user",
    )

    build_linkage_vector = SSHGCEOperator(
        task_id="build_linkage_vector",
        instance_name="{{ params.instance_name }}",
        base_dir=DAG_CONFIG["BASE_DIR"],
        command="python build_semantic_space.py "
        f"""--input-path {os.path.join(
                    DAG_CONFIG["STORAGE_PATH"], DAG_CONFIG["INPUT_SOURCES_DIR"],"data-*.parquet"
                )} """
        f"--reduction {DAG_CONFIG['REDUCTION']} "
        f"--batch-size {DAG_CONFIG['BATCH_SIZE']} ",
    )

    get_linkage_candidates = SSHGCEOperator(
        task_id="get_linkage_candidates",
        instance_name="{{ params.instance_name }}",
        base_dir=DAG_CONFIG["BASE_DIR"],
        command="python linkage_candidates.py "
        f"--batch-size {DAG_CONFIG['BATCH_SIZE']} "
        f"--reduction {DAG_CONFIG['REDUCTION']} "
        f"""--input-path {os.path.join(
                    DAG_CONFIG["STORAGE_PATH"],  DAG_CONFIG["INPUT_CANDIDATES_DIR"],"data-*.parquet"
                )} """
        f"--output-path {os.path.join(DAG_CONFIG['STORAGE_PATH'],DAG_CONFIG['LINKAGE_CANDIDATES_FILENAME'])} ",
    )

    link_items = SSHGCEOperator(
        task_id="link_items",
        instance_name="{{ params.instance_name }}",
        base_dir=DAG_CONFIG["BASE_DIR"],
        command="python link_items.py "
        f"""--input-sources-path {os.path.join(
                    DAG_CONFIG["STORAGE_PATH"], DAG_CONFIG["INPUT_SOURCES_DIR"]
                )} """
        f"""--input-candidates-path {os.path.join(
                    DAG_CONFIG["STORAGE_PATH"],  DAG_CONFIG["INPUT_CANDIDATES_DIR"]
                )} """
        f"--linkage-candidates-path {os.path.join(DAG_CONFIG['STORAGE_PATH'],DAG_CONFIG['LINKAGE_CANDIDATES_FILENAME'])} "
        f"--output-path {os.path.join(DAG_CONFIG['STORAGE_PATH'],DAG_CONFIG['LINKED_ITEMS_FILENAME'])} ",
    )

    load_link_items_into_bq = GCSToBigQueryOperator(
        bucket=MLFLOW_BUCKET_NAME,
        task_id="load_linked_artists_into_bq",
        source_objects=os.path.join(
            GCS_FOLDER_PATH, DAG_CONFIG["LINKED_ITEMS_FILENAME"]
        ),
        destination_project_dataset_table=f"{BIGQUERY_SANDBOX_DATASET}.{DAG_CONFIG['LINKED_ITEMS_TABLE']}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    (
        start
        >> import_data_tasks
        >> end_imports
        >> export_to_bq_tasks
        >> end_exports
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> build_linkage_vector
        >> get_linkage_candidates
        >> link_items
        >> load_link_items_into_bq
        >> gce_instance_stop
    )
