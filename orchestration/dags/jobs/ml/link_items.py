import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from common import macros
from common.config import (
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

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/linkage_item_{ENV_SHORT_NAME}/linkage_{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/item_linkage/",
    "EXPERIMENT_NAME": f"linkage_semantic_vector_v1.0_{ENV_SHORT_NAME}",
    "MODEL_NAME": f"v1.1_{ENV_SHORT_NAME}",
    "linkage_item_sources_data_request": "linkage_item_sources_data.sql",
    "linkage_item_candidates_data_request": "linkage_item_candidates_data.sql",
    "input_sources_table": f"{DATE}_input_sources_table",
    "input_candidates_table": f"{DATE}_item_candidates_data",
    "linked_items_table": "linked_items",
    "input_sources_filename": "item_sources_data.parquet",
    "input_candidates_filename": "item_candidates_data.parquet",
    "linkage_candidates_filename": "linkage_candidates_items.parquet",
    "linked_items_filename": "linkage_candidates_items.parquet",
}

gce_params = {
    "instance_name": f"linkage-item-{ENV_SHORT_NAME}",
    "instance_type": {
        "prod": "n1-standard-16",
        "stg": "n1-standard-8",
        "dev": "n1-standard-2",
    },
}

default_args = {
    "start_date": datetime(2022, 11, 30),
    # "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

schedule_dict = {"prod": "0 4 * * 3", "stg": "0 6 * * 3", "dev": "0 6 * * 3"}

with DAG(
    "link_items",
    default_args=default_args,
    description="Process to link items using semantic vectors.",
    schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
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
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"],
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    import_data_tasks = []
    import_sources = BigQueryExecuteQueryOperator(
        task_id="import_sources",
        sql=(
            IMPORT_LINKAGE_SQL_PATH / dag_config["linkage_item_sources_data_request"]
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{dag_config['input_sources_table']}",
    )
    import_data_tasks.append(import_sources)

    import_candidates = BigQueryExecuteQueryOperator(
        task_id="import_candidates",
        sql=(
            IMPORT_LINKAGE_SQL_PATH / dag_config["linkage_item_candidates_data_request"]
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{dag_config['input_candidates_table']}",
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
                    "tableId": f"{dag_config['input_sources_table']}",
                },
                "compression": None,
                "destinationFormat": "PARQUET",
                "destinationUris": os.path.join(
                    dag_config["STORAGE_PATH"], dag_config["input_sources_filename"]
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
                    "tableId": dag_config["input_candidates_table"],
                },
                "compression": None,
                "destinationFormat": "PARQUET",
                "destinationUris": os.path.join(
                    dag_config["STORAGE_PATH"], dag_config["input_candidates_filename"]
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
        labels={"job_type": "ml"},
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
        base_dir=dag_config["BASE_DIR"],
        command="pip install -r requirements.txt --user",
    )

    build_linkage_vector = SSHGCEOperator(
        task_id="build_linkage_vector",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python build_semantic_space.py "
        f"--input-path {os.path.join(dag_config['STORAGE_PATH'],dag_config['input_sources_filename'])}",
    )

    get_linkage_candidates = SSHGCEOperator(
        task_id="get_linkage_candidates",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python linkage_candidates.py "
        f"--input-path {os.path.join(dag_config['STORAGE_PATH'],dag_config['input_candidates_filename'])}"
        f"--output-table-path {os.path.join(dag_config['STORAGE_PATH'],dag_config['linkage_candidates_filename'])}",
    )

    link_items = SSHGCEOperator(
        task_id="link_items",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python link_items.py "
        f"--source-gcs-path {dag_config['STORAGE_PATH']} "
        "--sources-table-name {{ params.input_sources_table }} "
        "--candidates-table-name {{ params.input_candidates_table }} "
        "--input-table-path {{ params.output_linkage_candidates_table_path }} "
        "--output-table-path {{ params.linked_items_table }}",
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
        >> gce_instance_stop
    )
