from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common import macros
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    MLFLOW_BUCKET_NAME,
    BIGQUERY_TMP_DATASET,
)

from common.alerts import task_fail_slack_alert
from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"emb-reduction-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/reduction"
DATE = "{{ yyyymmdd(ds) }}"

default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/embedding_reduction_items_{ENV_SHORT_NAME}/embedding_reduction_items_{DATE}/{DATE}_item_embbedding_data",
}


with DAG(
    "embedding_reduction_items",
    default_args=default_args,
    description="Reduce embeddings",
    schedule_interval=get_airflow_schedule("0 0 * * 0"),  # every sunday
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
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-64",
            type="string",
        ),
        "reduction_config_file_name": Param(
            default="default-config",
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        preemptible=False,
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "long_ml"},
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
        "--config-file-name {{ params.reduction_config_file_name }} "
        f"--source-gs-path {dag_config['STORAGE_PATH']} "
        f"--output-table-name item_embeddings "
        f"--reduction-config default ",
        retries=2,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> export_task
        >> export_bq
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> reduce_dimension
        >> gce_instance_stop
        >> end
    )
