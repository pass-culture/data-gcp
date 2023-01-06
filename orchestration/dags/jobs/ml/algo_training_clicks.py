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
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    MLFLOW_BUCKET_NAME,
    SLACK_CONN_ID,
    SLACK_CONN_PASSWORD,
    MLFLOW_URL,
)

from dependencies.ml.utils import create_algo_training_slack_block

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_clicks_v2_{DATE}",
    "BASE_DIR": f"data-gcp/algo_training",
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
        "dev": "n2-standard-2",
        "stg": "c2-standard-16",
        "prod": "c2-standard-30",
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
        "event_day_number": Param(
            default=str(train_params["event_day_number"]),
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

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

    data_collect = SSHGCEOperator(
        task_id="data_collect",
        instance_name=gce_params["instance_name"],
        base_dir=dag_config["BASE_DIR"],
        export_config=dag_config,
        command="python data_collect.py "
        "--table-name training_data_clicks "
        "--event-day-number {{ params.event_day_number }}",
        dag=dag,
    )

    split_data = SSHGCEOperator(
        task_id="split_data",
        instance_name=gce_params["instance_name"],
        base_dir=dag_config["BASE_DIR"],
        export_config=dag_config,
        command="python split_data.py",
        dag=dag,
    )

    preprocess = SSHGCEOperator(
        task_id="preprocess",
        instance_name=gce_params["instance_name"],
        base_dir=dag_config["BASE_DIR"],
        export_config=dag_config,
        command="python preprocess.py",
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
        command=f"python evaluate.py",
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
        blocks=create_algo_training_slack_block(
            dag_config["EXPERIMENT_NAME"], MLFLOW_URL, ENV_SHORT_NAME
        ),
        username=f"Algo trainer robot - {ENV_SHORT_NAME}",
        icon_emoji=":robot_face:",
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> data_collect
        >> preprocess
        >> split_data
        >> training
        >> evaluate
        >> gce_instance_stop
        >> send_slack_notif_success
    )
