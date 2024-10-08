from datetime import datetime, timedelta

from common import macros
from common.alerts import task_fail_slack_alert
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
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/retrieval_vector_{ENV_SHORT_NAME}/semantic_{DATE}/{DATE}_item_embbedding_data",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/retrieval_vector/",
    "EXPERIMENT_NAME": f"retrieval_semantic_vector_v0.1_{ENV_SHORT_NAME}",
    "MODEL_NAME": f"v1.1_{ENV_SHORT_NAME}",
}

gce_params = {
    "instance_name": f"retrieval-semantic-vector-{ENV_SHORT_NAME}",
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
    "retrieval_semantic_vector_build",
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
        task_id="import_retrieval_semantic_vector_table",
        sql=(IMPORT_TRAINING_SQL_PATH / "retrieval_semantic_vector.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_retrieval_semantic_vector_data",
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
    export_bq = BigQueryInsertJobOperator(
        task_id="store_item_embbedding_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": f"{DATE}_retrieval_semantic_vector_data",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    sim_offers = SSHGCEOperator(
        task_id="containerize_retrieval_semantic_vector",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="python deploy_semantic.py "
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
