from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_FEATURES_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    ML_BUCKET_TEMP,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

DEFAULT_REGION = "europe-west1"
VERSION_SUFFIX = "_v2"
GCE_INSTANCE = f"emb-reduction{VERSION_SUFFIX.replace('_', '-')}-{ENV_SHORT_NAME}"
BASE_PATH = f"data-gcp/jobs/ml_jobs/reduction{VERSION_SUFFIX}"
DATE = "{{ yyyymmdd(ds) }}"
DAG_NAME = f"embedding_reduction_item{VERSION_SUFFIX}"
TABLE_ID = f"item_embedding{VERSION_SUFFIX}"

default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag_config = {
    "STORAGE_PATH": f"gs://{ML_BUCKET_TEMP}/embedding_reduction_items{VERSION_SUFFIX}_{ENV_SHORT_NAME}/run_{DATE}",
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Reduce embeddings",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
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
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        preemptible=False,
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "long_ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
    )

    export_bq = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="store_item_embbedding_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_FEATURES_DATASET,
                    "tableId": TABLE_ID,
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
        base_dir=BASE_PATH,
        environment=dag_config,
        command="PYTHONPATH=. python dimension_reduction.py "
        "--config-file-name {{ params.reduction_config_file_name }} "
        f"--source-gs-path {dag_config['STORAGE_PATH']} "
        f"--output-dataset-name ml_preproc_{ENV_SHORT_NAME} "
        f"--output-prefix-table-name {TABLE_ID} "
        f"--reduction-config default ",
        retries=2,
        deferrable=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> export_bq
        >> gce_instance_start
        >> fetch_install_code
        >> reduce_dimension
        >> gce_instance_stop
        >> end
    )
