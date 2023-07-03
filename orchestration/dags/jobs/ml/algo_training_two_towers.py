from datetime import timedelta

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
    BigQueryInsertJobOperator,
)
from common.utils import get_airflow_schedule
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    MLFLOW_BUCKET_NAME,
    SLACK_CONN_ID,
    SLACK_CONN_PASSWORD,
    MLFLOW_URL,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
)

from dependencies.ml.utils import create_algo_training_slack_block
from datetime import datetime

from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

DATE = "{{ ts_nodash }}"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_two_towers_{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/algo_training",
    "MODEL_DIR": "two_towers_model",
    "TRAIN_DIR": "/home/airflow/train",
    "EXPERIMENT_NAME": f"algo_training_two_towers_v1.1_{ENV_SHORT_NAME}",
}

# Params
train_params = {
    "config_file_name": "default-features",
    "batch_size": 8192,
    "validation_steps_ratio": 0.1 if ENV_SHORT_NAME == "prod" else 0.4,
    "embedding_size": 64,
    "train_set_size": 0.95 if ENV_SHORT_NAME == "prod" else 0.8,
    "event_day_number": {"prod": 60, "dev": 365, "stg": 20}[ENV_SHORT_NAME],
}
gce_params = {
    "instance_name": f"algo-training-two-towers-{ENV_SHORT_NAME}",
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

schedule_dict = {"prod": "0 12 * * 4", "dev": None, "stg": "0 12 * * 3"}


with DAG(
    "algo_training_two_towers",
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
        "config_file_name": Param(
            default=train_params["config_file_name"],
            type="string",
        ),
        "batch_size": Param(
            default=str(train_params["batch_size"]),
            type="string",
        ),
        "validation_steps_ratio": Param(
            default=str(train_params["validation_steps_ratio"]),
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
        "input_type": Param(
            default="enriched_clicks",
            type="string",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"]
            + "-"
            + train_params["config_file_name"],
            type="string",
        ),
        "run_name": Param(
            default=train_params["config_file_name"], type=["string", "null"]
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    import_tables = {}
    for table in [
        "recommendation_user_features",
        "recommendation_item_features",
        "training_data_enriched_clicks",
    ]:
        import_tables[table] = BigQueryExecuteQueryOperator(
            task_id=f"import_{table}",
            sql=(IMPORT_TRAINING_SQL_PATH / f"{table}.sql").as_posix(),
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.{table}",
            dag=dag,
        )

    # The params.input_type tells the .sql files which table to take as input
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
        import_tables[dataset] = task

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        accelerator_types=[{"name": "nvidia-tesla-t4", "count": 1}],
        retries=2,
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
                    "destinationUris": f"{dag_config['STORAGE_PATH']}/raw_recommendation_{split}_data/data-*.parquet",
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

    preprocess_data = {}
    for split in ["training", "validation", "test"]:
        preprocess_data[split] = SSHGCEOperator(
            task_id=f"preprocess_{split}",
            instance_name="{{ params.instance_name }}",
            base_dir=dag_config["BASE_DIR"],
            environment=dag_config,
            command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/preprocess.py "
            "--config-file-name {{ params.config_file_name }} "
            f"--input-dataframe-file-name raw_recommendation_{split}_data "
            f"--output-dataframe-file-name recommendation_{split}_data",
            dag=dag,
        )

    train = SSHGCEOperator(
        task_id="train",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/train.py "
        "--config-file-name {{ params.config_file_name }} "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} "
        "--batch-size {{ params.batch_size }} "
        "--validation-steps-ratio {{ params.validation_steps_ratio }} "
        "--embedding-size {{ params.embedding_size }} "
        "--seed {{ ds_nodash }} "
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
        "--experiment-name similar_offers_two_towers_v1.1_" + f"{ENV_SHORT_NAME} "
        "--model-name similar_offers_two_towers_v1.1",
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
        >> [
            import_tables["recommendation_user_features"],
            import_tables["recommendation_item_features"],
        ]
        >> import_tables["training_data_enriched_clicks"]
        >> import_tables["training"]
        >> [import_tables["validation"], import_tables["test"]]
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> [
            store_data["training"],
            store_data["validation"],
            store_data["test"],
            store_data["bookings"],
        ]
        >> preprocess_data["training"]
        >> preprocess_data["validation"]
        >> preprocess_data["test"]
        >> train
        >> evaluate
        >> train_sim_offers
        >> gce_instance_stop
        >> send_slack_notif_success
    )
