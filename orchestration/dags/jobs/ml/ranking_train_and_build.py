from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_RECOMMENDATION_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MLFLOW_BUCKET_NAME,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DATE = "{{ ts_nodash }}"
DEFAULT_REGION = "europe-west1"
BASE_PATH = "data-gcp/jobs/ml_jobs/ranking_endpoint"
DAG_NAME = "ranking_train_and_build"
RANKING_TRAINING_TABLE = "ranking_training_data"
STORAGE_PATH = f"gs://{MLFLOW_BUCKET_NAME}/ranking_training_{ENV_SHORT_NAME}/{DATE}"
TRAINING_DATA_DIR = f"{STORAGE_PATH}/training_data"

gce_params = {
    "instance_name": f"ranking-train-and-build-{ENV_SHORT_NAME}",
    "experiment_name": f"ranking_training_v1.0_{ENV_SHORT_NAME}",
    "model_name": f"v1.0_{ENV_SHORT_NAME}",
    "run_name": "default",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-4",
        "prod": "n1-standard-16",
    },
}
schedule_dict = {"prod": "0 20 * * 5", "dev": "0 20 * * *", "stg": "0 20 * * 3"}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Train and build Ranking",
    schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
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
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"],
            type="string",
        ),
        "dataset_name": Param(
            default=BIGQUERY_ML_RECOMMENDATION_DATASET, type="string"
        ),
        "table_name": Param(default=RANKING_TRAINING_TABLE, type="string"),
        "experiment_name": Param(default=gce_params["experiment_name"], type="string"),
        "run_name": Param(default=gce_params["run_name"], type="string"),
        "model_name": Param(default=gce_params["model_name"], type="string"),
    },
) as dag:
    download_data = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="store_data_to_gcs",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": "{{ params.dataset_name }}",
                    "tableId": "{{ params.table_name }}",
                },
                "compression": None,
                "destinationUris": f"{TRAINING_DATA_DIR}/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
    )

    train_model_and_build_docker_image = SSHGCEOperator(
        task_id="train_model_and_build_docker_image",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_PATH,
        command="python deploy_model.py "
        "--experiment-name {{ params.experiment_name }} "
        "--run-name {{ params.run_name }} "
        "--model-name {{ params.model_name }} "
        f"--input-gcs-dir {TRAINING_DATA_DIR}  ",
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    download_data >> train_model_and_build_docker_image
    (
        gce_instance_start
        >> fetch_install_code
        >> train_model_and_build_docker_image
        >> gce_instance_stop
    )
