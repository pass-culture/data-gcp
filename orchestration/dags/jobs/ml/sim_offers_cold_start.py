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
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/sim_offers_{ENV_SHORT_NAME}/cold_start_{DATE}/{DATE}_item_embbedding_data",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/algo_training/similar_offers",
    "EXPERIMENT_NAME": f"similar_offers_cold_start_v0.1_{ENV_SHORT_NAME}",
    "MODEL_NAME": f"v1.1_{ENV_SHORT_NAME}",
}

gce_params = {
    "instance_name": f"similar-offers-cold-start-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-highmem-16",
        "prod": "n1-highmem-32",
    },
}

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

schedule_dict = {"prod": "0 12 * * 4", "dev": None, "stg": "0 12 * * 3"}


with DAG(
    "similar_offers_cold_start",
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
        "experiment_name": Param(
            default=dag_config["EXPERIMENT_NAME"],
            type="string",
        ),
        "model_name": Param(
            default=dag_config["MODEL_NAME"],
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    export_task = BigQueryExecuteQueryOperator(
        task_id=f"import_tmp_similar_offers_cold_start_data_table",
        sql=(IMPORT_TRAINING_SQL_PATH / f"similar_offers_cold_start.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_similar_offers_cold_start_data",
        dag=dag,
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
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
    export_bq = BigQueryInsertJobOperator(
        task_id=f"store_item_embbedding_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": f"{DATE}_similar_offers_cold_start_data",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    sim_offers = SSHGCEOperator(
        task_id="containerize_similar_offers",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python deploy_cold_start.py "
        "--experiment-name {{ params.experiment_name }} "
        "--model-name {{ params.model_name }} "
        f"--source-gs-path {dag_config['STORAGE_PATH']}",
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    (
        start
        >> export_task
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> export_bq
        >> sim_offers
        >> gce_instance_stop
    )
