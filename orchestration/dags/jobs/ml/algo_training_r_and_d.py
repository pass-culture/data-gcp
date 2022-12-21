from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    GCE_TRAINING_INSTANCE,
    GCE_ZONE,
    MLFLOW_BUCKET_NAME,
    SLACK_BLOCKS,
    SLACK_CONN_ID,
    SLACK_CONN_PASSWORD,
)

from common.operator import GCloudComputeSSHOperator


DATE = "{{ts_nodash}}"

# Environment variables to export before running commands
DAG_CONFIG = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_{DATE}",
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "MODEL_NAME": "events",
    "TRAIN_DIR": "/home/airflow/train",
    "EXPERIMENT_NAME": f"algo_training_events.1_{ENV_SHORT_NAME}",
}


default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "algo_training_r_and_d",
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
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = ComputeEngineStartInstanceOperator(
        task_id="gce_start_task",
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_TRAINING_INSTANCE,
        dag=dag,
    )

    fetch_code = GCloudComputeSSHOperator(
        task_id="fetch_code",
        command=r'if cd data-gcp; then git checkout master && git pull && git checkout {{ params.branch }} && git pull; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp && git checkout {{ params.branch }} && git pull; fi',
        dag=dag,
    )

    install_dependencies = GCloudComputeSSHOperator(
        task_id="install_dependencies",
        dag_config=DAG_CONFIG,
        path_to_run_command="data-gcp/algo_training",
        command="pip install -r requirements.txt --user",
        dag=dag,
    )

    training = GCloudComputeSSHOperator(
        task_id="training",
        dag_config=DAG_CONFIG,
        path_to_run_command="data-gcp/algo_training",
        command=f"python train_v1_1.py",
        dag=dag,
    )

    postprocess = GCloudComputeSSHOperator(
        task_id="postprocess",
        dag_config=DAG_CONFIG,
        path_to_run_command="data-gcp/algo_training",
        command=f"python postprocess.py",
        dag=dag,
    )

    evaluate = GCloudComputeSSHOperator(
        task_id="evaluate",
        dag_config=DAG_CONFIG,
        path_to_run_command="data-gcp/algo_training",
        command=f"python evaluate.py",
        dag=dag,
    )

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        task_id="gce_stop_task",
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_TRAINING_INSTANCE,
    )

    # send_slack_notif_success = SlackWebhookOperator(
    #     task_id="send_slack_notif_success",
    #     http_conn_id=SLACK_CONN_ID,
    #     webhook_token=SLACK_CONN_PASSWORD,
    #     blocks=SLACK_BLOCKS,
    #     username=f"Algo trainer robot - {ENV_SHORT_NAME}",
    #     icon_emoji=":robot_face:",
    # )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> training
        >> evaluate
        >> gce_instance_stop
        # >> send_slack_notif_success
    )
