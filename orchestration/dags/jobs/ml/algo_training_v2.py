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
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    MLFLOW_BUCKET_NAME,
    SLACK_CONN_ID,
    SLACK_CONN_PASSWORD,
    BIGQUERY_RAW_DATASET,
    MLFLOW_URL,
)

from dependencies.ml.utils import create_algo_training_slack_block
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_v2-{DATE}",
    "BASE_DIR": f"data-gcp/algo_training",
    "TRAIN_DIR": "/home/airflow/train",
    "EXPERIMENT_NAME": f"algo_training_v2.1_{ENV_SHORT_NAME}",
}

# Params
train_params = {
    "batch_size": 4096,
    "embedding_size": 64,
    "train_set_size": 0.8,
}
gce_params = {
    "instance_name": f"algo-training-v2-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n2-standard-2",
        "stg": "c2-standard-8",
        "prod": "c2-standard-16",
    },
}

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "algo_training_v2",
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
        "batch_size": Param(
            default=str(train_params["batch_size"]),
            type="string",
        ),
        "embedding_size": Param(
            default=str(train_params["embedding_size"]),
            type="string",
        ),
        "train_set_size": Param(
            default=str(train_params["train_set_size"]),
            type="string",
        ),
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
            destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.recommendation_{dataset}_data",
            dag=dag,
        )
        import_recommendation_data[dataset] = task

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=gce_params["instance_name"],
        machine_type=gce_params["instance_type"][ENV_SHORT_NAME],
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=gce_params["instance_name"],
        branch="{{ params.branch }}",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=gce_params["instance_name"],
        base_dir=dag_config["BASE_DIR"],
        command="pip install -r requirements.txt --user",
        dag=dag,
    )

    training = SSHGCEOperator(
        task_id="training",
        instance_name=gce_params["instance_name"],
        base_dir=dag_config["BASE_DIR"],
        export_config=dag_config,
        command=f"python train_v2.py "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} "
        "--batch-size {{ params.batch_size }} "
        "--embedding-size {{ params.embedding_size }} "
        "--seed {{ ds_nodash }}",
        dag=dag,
    )

    evaluate = SSHGCEOperator(
        task_id="evaluate",
        instance_name=gce_params["instance_name"],
        base_dir=dag_config["BASE_DIR"],
        export_config=dag_config,
        command=f"python evaluate_v2.py --experiment-name {dag_config['EXPERIMENT_NAME']}",
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task",
        instance_name=gce_params["instance_name"],
    )

    send_slack_notif_success = SlackWebhookOperator(
        task_id="send_slack_notif_success",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_CONN_PASSWORD,
        blocks=create_algo_training_slack_block(MLFLOW_URL, ENV_SHORT_NAME),
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
        >> training
        >> evaluate
        >> gce_instance_stop
        >> send_slack_notif_success
    )
