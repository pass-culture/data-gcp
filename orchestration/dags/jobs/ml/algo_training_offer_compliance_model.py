import json
from datetime import datetime, timedelta

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_ML_COMPLIANCE_DATASET,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MLFLOW_BUCKET_NAME,
    MLFLOW_URL,
    SLACK_CONN_PASSWORD,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule
from dependencies.ml.utils import create_algo_training_slack_block

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.http.operators.http import HttpOperator

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_offer_compliance_model_v1.0_{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/algo_training",
    "MODEL_DIR": "fraud/offer_compliance_model",
    "TRAIN_DIR": "/home/airflow/train",
    "TOKENIZERS_PARALLELISM": "false",
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
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

schedule_dict = {"prod": "0 12 * * 5", "dev": "0 0 * * *", "stg": "0 12 * * 3"}


with DAG(
    "algo_training_offer_compliance_model",
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
    start = DummyOperator(task_id="start", dag=dag)

    import_offer_as_parquet = BigQueryInsertJobOperator(
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
        labels={"job_type": "ml"},
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
        command=f"mkdir -p img && PYTHONPATH=. python {dag_config['MODEL_DIR']}/preprocess.py "
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
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/split_data.py "
        f"--clean-table-name compliance_clean_data "
        "--training-table-name compliance_training_data "
        "--validation-table-name compliance_validation_data ",
        dag=dag,
    )

    train = SSHGCEOperator(
        task_id="train",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/train.py "
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
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/evaluate.py "
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
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/package_api_model.py "
        "--model-name {{ params.model_name }} "
        "--config-file-name {{ params.config_file_name }} ",
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    send_slack_notif_success = HttpOperator(
        task_id="send_slack_notif_success",
        method="POST",
        http_conn_id="http_slack_default",
        endpoint=f"{SLACK_CONN_PASSWORD}",
        data=json.dumps(
            {
                "blocks": create_algo_training_slack_block(
                    "{{ params.model_name }}", MLFLOW_URL, ENV_SHORT_NAME
                )
            }
        ),
        headers={"Content-Type": "application/json"},
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
