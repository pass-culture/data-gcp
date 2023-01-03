from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    GCE_TRAINING_INSTANCE,
    GCE_ZONE,
    MLFLOW_BUCKET_NAME,
    SLACK_BLOCKS,
    SLACK_CONN_ID,
    SLACK_CONN_PASSWORD,
    BIGQUERY_RAW_DATASET,
)

from common.operator import GCloudComputeSSHOperator
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

DATE = "{{ts_nodash}}"

# Environment variables to export before running commands
DAG_CONFIG = {
    "GCE_TRAINING_INSTANCE": "algo-training-dev-1",
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_{DATE}",
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "TRAIN_DIR": "/home/airflow/train",
    "EXPERIMENT_NAME": f"algo_training_v2.1_{ENV_SHORT_NAME}",
    "BATCH_SIZE": 1024,
    "EMBEDDING_SIZE": 64,
    "TRAIN_SET_SIZE": 0.8,
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
        "gce_training_instance": Param(
            default=str(DAG_CONFIG["GCE_TRAINING_INSTANCE"]),
            type="string",
        ),
        "batch_size": Param(
            default=str(DAG_CONFIG["BATCH_SIZE"]),
            type="string",
        ),
        "embedding_size": Param(
            default=str(DAG_CONFIG["EMBEDDING_SIZE"]),
            type="string",
        ),
        "train_set_size": Param(
            default=str(DAG_CONFIG["TRAIN_SET_SIZE"]),
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    import_recommendation_data = {}
    for dataset in ["data", "training_data", "validation_data", "test_data"]:
        task = BigQueryExecuteQueryOperator(
            task_id=f"import_recommendation_{dataset}",
            sql=(IMPORT_TRAINING_SQL_PATH / f"recommendation_{dataset}.sql").as_posix(),
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.recommendation_{dataset}",
            dag=dag,
        )
        import_recommendation_data[dataset] = task

    gce_instance_start = ComputeEngineStartInstanceOperator(
        task_id="gce_start_task",
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=DAG_CONFIG["GCE_TRAINING_INSTANCE"],
        dag=dag,
    )

    fetch_code = GCloudComputeSSHOperator(
        task_id="fetch_code",
        command=r"if cd data-gcp; then git fetch --all && git reset --hard origin/{{ params.branch }}; "
        r"else git clone git@github.com:pass-culture/data-gcp.git "
        r"&& cd data-gcp && git checkout {{ params.branch }} && git pull; fi",
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
        command=f"python train_v2.py "
        f"--experiment-name {DAG_CONFIG['EXPERIMENT_NAME']} "
        r"--batch-size {{ params.batch_size }}  "
        r"--embedding-size {{ params.embedding_size }} "
        r"--seed {{ ds_nodash }}",
        dag=dag,
    )

    evaluate = GCloudComputeSSHOperator(
        task_id="evaluate",
        dag_config=DAG_CONFIG,
        path_to_run_command="data-gcp/algo_training",
        command=f"python evaluate_v2.py",
        dag=dag,
    )

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        task_id="gce_stop_task",
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=DAG_CONFIG["GCE_TRAINING_INSTANCE"],
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
        >> import_recommendation_data["data"]
        >> import_recommendation_data["training_data"]
        >> import_recommendation_data["validation_data"]
        >> import_recommendation_data["test_data"]
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> training
        >> evaluate
        >> gce_instance_stop
        # >> send_slack_notif_success
    )
