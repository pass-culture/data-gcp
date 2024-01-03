from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.operators.biquery import bigquery_job_task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dependencies.ml.embeddings.import_items import params
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, DAG_FOLDER
from common.utils import get_airflow_schedule
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    MLFLOW_BUCKET_NAME,
    BIGQUERY_TMP_DATASET,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"extract-items-embeddings-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/embeddings"
DATE = "{{ yyyymmdd(ds) }}"
default_args = {
    "start_date": datetime(2023, 9, 6),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/embedding_extraction_items_{ENV_SHORT_NAME}/embedding_extraction_items_{DATE}/{DATE}_item_embbedding_data",
    "TOKENIZERS_PARALLELISM": "false",
}
with DAG(
    "embeddings_extraction_items",
    default_args=default_args,
    description="Extact items metadata embeddings",
    schedule_interval=get_airflow_schedule("0 0 * * *"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-32",
            type="string",
        ),
        "config_file_name": Param(
            default="default-config-item",
            type="string",
        ),
        "batch_size": Param(
            default=20000 if ENV_SHORT_NAME == "prod" else 10000,
            type="integer",
        ),
    },
) as dag:
    data_collect_task = bigquery_job_task(
        dag, "import_item_batch", params, extra_params={}
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "ml"},
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        python_version="3.10",
        command="{{ params.branch }}",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
    )

    preprocess = SSHGCEOperator(
        task_id="preprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python preprocess.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} "
        f"--input-table-name {DATE}_item_to_extract_embeddings "
        f"--output-table-name {DATE}_item_to_extract_embeddings_clean ",
    )

    extract_embedding = SSHGCEOperator(
        task_id="extract_embedding",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command="mkdir -p img && PYTHONPATH=. python main.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} "
        f"--input-table-name {DATE}_item_to_extract_embeddings_clean "
        f"--output-table-name item_embeddings",
    )

    export_task = BigQueryExecuteQueryOperator(
        task_id=f"import_item_embbedding_data",
        sql=(IMPORT_TRAINING_SQL_PATH / f"item_embeddings_reduction.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_item_embeddings_reduction",
        dag=dag,
    )

    export_bq = BigQueryInsertJobOperator(
        task_id=f"store_item_embbedding_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": f"{DATE}_item_embeddings_reduction",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    reduce_dimension = SSHGCEOperator(
        task_id="reduce_dimension",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command="PYTHONPATH=. python dimension_reduction.py "
        f"--gcp-project {GCP_PROJECT_ID} "
        f"--env-short-name {ENV_SHORT_NAME} "
        "--config-file-name {{ params.config_file_name }} "
        f"--source-gs-path {dag_config['STORAGE_PATH']} "
        f"--output-table-name item_embeddings_reduced",
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        data_collect_task
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> preprocess
        >> extract_embedding
        >> export_task
        >> export_bq
        >> reduce_dimension
        >> gce_instance_stop
    )
