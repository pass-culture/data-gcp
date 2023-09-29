from datetime import datetime
from datetime import timedelta

from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow import DAG
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
)
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)


DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "BASE_DIR": "data-gcp/jobs/playground_vm",
}

# Params
# default instance type prod :  "n1-highmem-32"
gce_params = {
    "instance_name": f"playground-vm-YourName-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-highmem-8",
        "prod": "n1-highmem-32",
    },
}

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "launch_vm",
    default_args=default_args,
    description="Launch a vm to work in production environment",
    schedule=None,
    catchup=False,
    dagrun_timeout=None,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"]["prod"], type="string"
        ),
        "instance_name": Param(default=gce_params["instance_name"], type="string"),
        "gpu_count": Param(default=0, type="int"),
        "keep_alive": Param(default="true", type="string"),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        labels={"keep_alive": "{{ params.keep_alive }}"},
        accelerator_types="[]"
        if params.gpu_count <= 0
        else str(
            eval([{"name": "nvidia-tesla-t4", "count": "{{ params.gpu_count }}"}])
        ),
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
        retries=2,
    )

    store_data = {}
    for split in ["training", "validation", "test"]:
        store_data[split] = BigQueryInsertJobOperator(
            task_id=f"store_{split}_data",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": f"{DATE}_recommendation_{split}_data",
                    },
                    "compression": None,
                    "destinationUris": f"{dag_config['STORAGE_PATH']}/recommendation_{split}_data/data-*.parquet",
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

    store_data["bookings"] = BigQueryInsertJobOperator(
        task_id=f"store_bookings_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_RAW_DATASET,
                    "tableId": f"training_data_bookings",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/bookings/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    train = SSHGCEOperator(
        task_id="train",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/train.py "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} "
        "--batch-size {{ params.batch_size }} "
        "--embedding-size {{ params.embedding_size }} "
        "--seed {{ ds_nodash }} "
        "--training-table-name recommendation_training_data "
        "--validation-table-name recommendation_validation_data "
        "--run-name {{ params.run_name }}",
        dag=dag,
    )

    evaluate = SSHGCEOperator(
        task_id="evaluate",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python evaluate.py "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} ",
        dag=dag,
    )

    train_sim_offers = SSHGCEOperator(
        task_id="containerize_similar_offers",
        instance_name="{{ params.instance_name }}",
        base_dir=f"{dag_config['BASE_DIR']}/similar_offers",
        environment=dag_config,
        command="python deploy_model.py "
        "--experiment-name similar_offers_{{ params.input_type }}"
        + f"_v2.1_{ENV_SHORT_NAME} "
        "--model-name v2.1 "
        f"--source-experiment-name {dag_config['EXPERIMENT_NAME']} ",
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    send_slack_notif_success = SlackWebhookOperator(
        task_id="send_slack_notif_success",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_CONN_PASSWORD,
        blocks=create_algo_training_slack_block(
            dag_config["EXPERIMENT_NAME"], MLFLOW_URL, ENV_SHORT_NAME
        ),
        username=f"Algo trainer robot - {ENV_SHORT_NAME}",
        icon_emoji=":robot_face:",
    )

    (start >> gce_instance_start >> fetch_code >> install_dependencies)
