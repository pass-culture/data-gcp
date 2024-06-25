from datetime import timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from common.utils import get_airflow_schedule
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    MLFLOW_BUCKET_NAME,
    BIGQUERY_TMP_DATASET,
)

from datetime import datetime

from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/linkage_item_{ENV_SHORT_NAME}/linkage_{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/item_linkage/",
    "EXPERIMENT_NAME": f"linkage_semantic_vector_v1.0_{ENV_SHORT_NAME}",
    "MODEL_NAME": f"v1.1_{ENV_SHORT_NAME}",
    "input_table_name": "item_data",
    "output_lancedb_path": "metadata/vector",
    "output_linkage_candidates_table_path": "linkage_candidates_items",
    "output_linkage_final_table_path": "linkage_final_items",
}

gce_params = {
    "instance_name": f"linkage-item-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-8",
        "prod": "n1-standard-16",
    },
}

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

schedule_dict = {"prod": "0 4 * * *", "dev": "0 6 * * *", "stg": "0 6 * * 3"}

with DAG(
    "link_items",
    default_args=default_args,
    description="Custom training job",
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
        "input_table_name": Param(
            default=dag_config["input_table_name"],
            type="string",
        ),
        "output_lancedb_path": Param(
            default=dag_config["output_lancedb_path"],
            type="string",
        ),
        "output_linkage_candidates_table_path": Param(
            default=dag_config["output_linkage_candidates_table_path"],
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    export_task = BigQueryExecuteQueryOperator(
        task_id=f"import_retrieval_semantic_vector_table",
        sql=(IMPORT_TRAINING_SQL_PATH / f"linkage_item_data.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_linkage_item_data",
        dag=dag,
    )

    export_bq = BigQueryInsertJobOperator(
        task_id=f"store_item_embbedding_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": f"{DATE}_linkage_item_data",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/{dag_config['input_table_name']}/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

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
        dag=dag,
    )
    
    build_linkage_vector = SSHGCEOperator(
        task_id="build_linkage_vector",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python build_semantic_space.py "
        f"--source-gcs-path {dag_config['STORAGE_PATH']} "
        "--input-table-name item_data ",
        dag=dag,
    )

    get_linkage_candidates = SSHGCEOperator(
        task_id="get_linkage_candidates",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python linkage_candidates.py "
        f"--model-path {dag_config['output_lancedb_path']} "
        f"--source-gcs-path {dag_config['STORAGE_PATH']} "
        "--input-table-name item_data "
        f"--output-table-path {dag_config['output_linkage_candidates_table_path']}",
        dag=dag,
    )

    link_items = SSHGCEOperator(
        task_id="link_items",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python link_items.py "
        f"--source-gcs-path {dag_config['STORAGE_PATH']} "
        f"--catalog-table-name item_data "
        f"--input-table-name {dag_config['output_linkage_candidates_table_path']} "
        f"--output-table-name {dag_config['output_linkage_final_table_path']}",
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    (
        start
        >> export_task
        >> export_bq
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> build_linkage_vector
        >> get_linkage_candidates
        >> link_items
        >> gce_instance_stop
    )
