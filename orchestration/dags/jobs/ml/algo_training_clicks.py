from datetime import datetime
from datetime import timedelta

from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from airflow import DAG
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    MLFLOW_BUCKET_NAME,
    SLACK_CONN_ID,
    SLACK_CONN_PASSWORD,
    MLFLOW_URL,
    BIGQUERY_TMP_DATASET,
)
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from dependencies.ml.utils import create_algo_training_slack_block
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_clicks_v2_{DATE}",
    "BASE_DIR": "data-gcp/algo_training",
    "TRAIN_DIR": "/home/airflow/train",
    "EXPERIMENT_NAME": f"algo_training_clicks_v2.1_{ENV_SHORT_NAME}",
}

# Params
train_params = {
    "batch_size": 4096,
    "embedding_size": 64,
    "train_set_size": 0.8,
    "event_day_number": 120 if ENV_SHORT_NAME == "prod" else 20,
}
gce_params = {
    "instance_name": f"algo-training-clicks-v2-{ENV_SHORT_NAME}",
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

with DAG(
    "algo_training_clicks_v2",
    default_args=default_args,
    description="Custom training job",
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "batch_size": Param(default=str(train_params["batch_size"]), type="string"),
        "embedding_size": Param(
            default=str(train_params["embedding_size"]), type="string"
        ),
        "train_set_size": Param(
            default=str(train_params["train_set_size"]), type="string"
        ),
        "event_day_number": Param(
            default=str(train_params["event_day_number"]), type="string"
        ),
        "input_type": Param(
            default="clicks",
            type="string",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME], type="string"
        ),
        "instance_name": Param(default=gce_params["instance_name"], type="string"),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    import_recommendation_data = {}
    for dataset in ["training", "validation", "test"]:
        task = BigQueryExecuteQueryOperator(
            task_id=f"import_recommendation_{dataset}",
            sql=(
                IMPORT_TRAINING_SQL_PATH / f"recommendation_{dataset}_data.sql"
            ).as_posix(),
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_recommendation_{dataset}_data_clicks",
            dag=dag,
        )
        import_recommendation_data[dataset] = task

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        accelerator_types=[{"name": "nvidia-tesla-t4", "count": 1}],
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name="{{ params.instance_name }}",
        command="{{ params.branch }}",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="pip install -r requirements.txt --user",
        dag=dag,
    )

    store_recommendation_data = {}
    for split in ["training", "validation", "test"]:
        task = SSHGCEOperator(
            task_id=f"store_recommendation_{split}",
            instance_name="{{ params.instance_name }}",
            base_dir=dag_config["BASE_DIR"],
            environment=dag_config,
            command=f"python data_collect.py --dataset {BIGQUERY_TMP_DATASET} "
            f"--table-name {DATE}_recommendation_{split}_data_clicks "
            f"--output-name recommendation_{split}_data",
            dag=dag,
        )
        store_recommendation_data[split] = task

    training = SSHGCEOperator(
        task_id="training",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"python train_v2.py "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} "
        "--batch-size {{ params.batch_size }} "
        "--embedding-size {{ params.embedding_size }} "
        "--seed {{ ds_nodash }} "
        f"--training-table-name recommendation_training_data "
        f"--validation-table-name recommendation_validation_data",
        dag=dag,
    )

    evaluate = SSHGCEOperator(
        task_id="evaluate",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command="python evaluate.py "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} "
        "--event-day-number {{ params.event_day_number }} "
        "--training-dataset-name recommendation_training_data "
        "--test-dataset-name recommendation_test_data",
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

    (
        start
        >> import_recommendation_data["training"]
        >> import_recommendation_data["validation"]
        >> import_recommendation_data["test"]
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> store_recommendation_data["training"]
        >> store_recommendation_data["validation"]
        >> store_recommendation_data["test"]
        >> training
        >> evaluate
        >> gce_instance_stop
        >> send_slack_notif_success
    )
