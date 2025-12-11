from datetime import datetime, timedelta

from common import macros
from common.alerts import SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN
from common.alerts.ml_training import create_algo_training_slack_block
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_COMPLIANCE_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    ML_BUCKET_TEMP,
    MLFLOW_URL,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.operators.slack import SendSlackMessageOperator
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

DATE = "{{ ts_nodash }}"
DAG_NAME = "algo_training_offer_compliance_model"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{ML_BUCKET_TEMP}/algo_training_{ENV_SHORT_NAME}/algo_training_offer_compliance_model_v1.0_{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/offer_compliance",
}

# Params
train_params = {
    "config_file_name": "default",
}
gce_params = {
    "instance_name": f"algo-training-offer-compliance-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-highmem-8",
        "prod": "n1-highmem-32",
    },
}

default_args = {
    "start_date": datetime(2023, 5, 9),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

schedule_dict = {"prod": "0 12 * * 5", "dev": None, "stg": "0 12 * * 3"}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Custom training job",
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
        "model_name": Param(
            default="compliance_default",
            type="string",
        ),
        "config_file_name": Param(
            default=train_params["config_file_name"],
            type="string",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME], type="string"
        ),
        "instance_name": Param(default=gce_params["instance_name"], type="string"),
        "run_name": Param(default="default", type=["string", "null"]),
    },
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    import_offer_as_parquet = BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id="import_offer_as_parquet",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_COMPLIANCE_DATASET,
                    "tableId": "training_data_offer",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/compliance_raw_data/data-*.parquet",
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
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=dag_config["BASE_DIR"],
        retries=2,
    )

    preprocess = SSHGCEOperator(
        task_id="preprocess",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command="mkdir -p img && PYTHONPATH=. python preprocess.py "
        "--config-file-name {{ params.config_file_name }} "
        "--input-dataframe-file-name compliance_raw_data "
        "--output-dataframe-file-name compliance_clean_data ",
        dag=dag,
    )

    split_data = SSHGCEOperator(
        task_id="split_data",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command="PYTHONPATH=. python split_data.py "
        "--clean-table-name compliance_clean_data "
        "--training-table-name compliance_training_data "
        "--validation-table-name compliance_validation_data ",
        dag=dag,
    )

    train = SSHGCEOperator(
        task_id="train",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command="PYTHONPATH=. python train.py "
        "--model-name {{ params.model_name }} "
        "--config-file-name {{ params.config_file_name }} "
        "--training-table-name compliance_training_data "
        "--run-name {{ params.run_name }}",
        dag=dag,
    )

    evaluate = SSHGCEOperator(
        task_id="evaluate",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command="PYTHONPATH=. python evaluate.py "
        "--model-name {{ params.model_name }} "
        "--config-file-name {{ params.config_file_name }} "
        "--validation-table-name compliance_validation_data "
        "--run-name {{ params.run_name }}",
        dag=dag,
    )

    package_api_model = SSHGCEOperator(
        task_id="package_api_model",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command="PYTHONPATH=. python package_api_model.py "
        "--model-name {{ params.model_name }} "
        "--config-file-name {{ params.config_file_name }} ",
        dag=dag,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    send_slack_notif_success = SendSlackMessageOperator(
        task_id="send_slack_notif_success",
        webhook_token=SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN,
        block=create_algo_training_slack_block(
            "{{ params.model_name }}", MLFLOW_URL, ENV_SHORT_NAME
        ),
    )

    (
        start
        >> import_offer_as_parquet
        >> gce_instance_start
        >> fetch_install_code
        >> preprocess
        >> split_data
        >> train
        >> evaluate
        >> package_api_model
        >> gce_instance_stop
        >> send_slack_notif_success
    )
