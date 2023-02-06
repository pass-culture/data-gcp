from datetime import datetime
from datetime import timedelta

from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from common.utils import get_airflow_schedule
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
    BIGQUERY_RAW_DATASET,
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
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_clicks_v2.1_{DATE}",
    "BASE_DIR": "data-gcp/algo_training",
    "MODEL_DIR": "triplet_model",
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
    "instance_name": f"algo-training-{ENV_SHORT_NAME}",
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

schedule_dict = {"prod": "0 12 * * 5", "dev": "0 0 * * *", "stg": "0 12 * * 3"}


with DAG(
    "algo_training",
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
        "run_name": Param(default=None, type=["string", "null"]),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    # The params.input_type tells the .sql files which table to take as input
    import_recommendation_data = {}
    for dataset in ["training", "validation", "test"]:
        task = BigQueryExecuteQueryOperator(
            task_id=f"import_tmp_{dataset}_table",
            sql=(
                IMPORT_TRAINING_SQL_PATH / f"recommendation_{dataset}_data.sql"
            ).as_posix(),
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_recommendation_{dataset}_data",
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

    store_data = {}
    for split in ["training", "validation", "test"]:
        store_data[split] = BigQueryToGCSOperator(
            task_id=f"store_{split}_data",
            source_project_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_recommendation_{split}_data",
            destination_cloud_storage_uris=f"{dag_config['STORAGE_PATH']}/recommendation_{split}_data/data-*.parquet",
            export_format="PARQUET",
            dag=dag,
        )

    store_data["bookings"] = BigQueryToGCSOperator(
        task_id=f"store_bookings_data",
        source_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.training_data_bookings",
        destination_cloud_storage_uris=f"{dag_config['STORAGE_PATH']}/bookings.parquet",
        export_format="PARQUET",
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
        command="python main.py "
        "--experiment-name similar_offers_{{ params.input_type }}"
        + f"_v2.1_{ENV_SHORT_NAME} "
        "--model-name v2.1",
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
        >> [
            import_recommendation_data["validation"],
            import_recommendation_data["test"],
        ]
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> [
            store_data["training"],
            store_data["validation"],
            store_data["test"],
            store_data["bookings"],
        ]
        >> train
        >> evaluate
        >> train_sim_offers
        >> gce_instance_stop
        >> send_slack_notif_success
    )
