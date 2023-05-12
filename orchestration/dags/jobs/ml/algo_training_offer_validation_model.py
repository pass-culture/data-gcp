from datetime import datetime
from datetime import timedelta

from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from common.utils import get_airflow_schedule
from airflow import DAG
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
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_offer_validation_model_v1.0_{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/algo_training",
    "MODEL_DIR": "fraud/offer_validation_model",
    "TRAIN_DIR": "/home/airflow/train",
    "EXPERIMENT_NAME": f"algo_training_offer_validation_model_v1.0_{ENV_SHORT_NAME}",
}

# Params
train_params = {
    "config_file_name": "default",
}
gce_params = {
    "instance_name": f"algo-training-{ENV_SHORT_NAME}",
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
    "algo_training_offer_validation_model",
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
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME], type="string"
        ),
        "instance_name": Param(default=gce_params["instance_name"], type="string"),
        "run_name": Param(default=None, type=["string", "null"]),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    import_tables = {}
    for table in [
        "validation_offers",
    ]:
        import_tables[table] = BigQueryExecuteQueryOperator(
            task_id=f"import_{table}",
            sql=(IMPORT_TRAINING_SQL_PATH / f"{table}.sql").as_posix(),
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{DATE}_{table}_raw_data",
            dag=dag,
        )

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
    store_data={}
    store_data["raw"] = BigQueryInsertJobOperator(
        task_id=f"store_raw_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_TMP_DATASET,
                    "tableId": f"{DATE}_validation_raw_data",
                },
                "compression": None,
                "destinationUris": f"{dag_config['STORAGE_PATH']}/validation_raw_data/data-*.parquet",
                "destinationFormat": "PARQUET",
            }
        },
        dag=dag,
    )

    preprocess = SSHGCEOperator(
        task_id="preprocess",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/preprocess.py "
        "--config-file-name {{ params.config_file_name }} "
        "--input-dataframe-file-name validation_raw_data "
        "--output-dataframe-file-name validation_clean_data ",
        dag=dag,
    )

    split_data = SSHGCEOperator(
        task_id="split_data",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/split_data.py "
        f"--clean-table-name validation_clean_data "
        "--training-table-name validation_training_data "
        "--validation-table-name validation_validation_data ",
        dag=dag,
    )

    train = SSHGCEOperator(
        task_id="train",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/train.py "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} "
        "--config-file-name {{ params.config_file_name }} "
        "--training-table-name validation_training_data "
        "--run-name {{ params.run_name }}",
        dag=dag,
    )

    evaluate = SSHGCEOperator(
        task_id="evaluate",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python evaluate.py "
        f"--experiment-name {dag_config['EXPERIMENT_NAME']} "
        "--config-file-name {{ params.config_file_name }} "
        "--validation-table-name validation_validation_data "
        "--run-name {{ params.run_name }}",
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
            import_tables["validation_offers"],
        ]
        >> [
            store_data["raw"],
        ]
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> preprocess
        >> split_data
        >> train
        >> evaluate
        >> gce_instance_stop
        >> send_slack_notif_success
    )
