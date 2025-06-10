from datetime import datetime, timedelta

from common import macros
from common.alerts import SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN
from common.alerts.ml_training import create_algo_training_slack_block
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_PREPROCESSING_DATASET,
    BIGQUERY_ML_RECOMMENDATION_DATASET,
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    INSTANCES_TYPES,
    MLFLOW_BUCKET_NAME,
    ML_BUCKET_TEMP,
    ML_BUCKET_PROCESSING,
    ML_BUCKET_OUTPUT,
    MLFLOW_URL,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.operators.slack import SendSlackMessageOperator
from common.utils import get_airflow_schedule
from jobs.crons import SCHEDULE_DICT
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup

DATE = "{{ ts_nodash }}"
DAG_NAME = "algo_training_two_towers"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/{DAG_NAME}_{DATE}",
    "TEMP_PATH": f"gs://{ML_BUCKET_TEMP}/algo_training_{ENV_SHORT_NAME}/{DAG_NAME}_{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/algo_training",
    "MODEL_DIR": "two_towers_model",
    "TRAIN_DIR": "/home/airflow/train",
}

# Params
train_params = {
    "config_file_name": {
        "prod": "default-features",
        "dev": "default-features",
        "stg": "default-features",
    }[ENV_SHORT_NAME],
    "run_name": "updated-loss-202502",
    "batch_size": {"prod": 2048, "dev": 8192, "stg": 4096}[ENV_SHORT_NAME],
    "embedding_size": 64,
    "train_set_size": 0.95 if ENV_SHORT_NAME == "prod" else 0.8,
    "event_day_number": {"prod": 90, "dev": 365, "stg": 30}[ENV_SHORT_NAME],
    "experiment_name": f"{DAG_NAME}_v1.2_{ENV_SHORT_NAME}",
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
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


def should_upload_embeddings(**kwargs):
    upload_embeddings = kwargs["params"].get("upload_embeddings", True)
    return "upload_embeddings" if upload_embeddings else "skip_upload_embeddings"


with (
    DAG(
        DAG_NAME,
        default_args=default_args,
        description="Custom training job",
        schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME]),
        catchup=False,
        dagrun_timeout=timedelta(minutes=1440),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
        render_template_as_native_obj=True,  # be careful using this because "3.10" is rendered as 3.1 if not double escaped
        doc_md="This DAG is used to train a two-towers model. It takes the data from ml_reco__training_data_click which is computed every day.",
        tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
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
            # TODO: Voir si on peut le supprimer, sinon mettre un enum
            "input_type": Param(
                default="click",
                type="string",
            ),
            "instance_type": Param(
                default=gce_params["instance_type"][ENV_SHORT_NAME],
                type="string",
            ),
            "gpu_type": Param(
                default="nvidia-tesla-t4", enum=INSTANCES_TYPES["gpu"]["name"]
            ),
            "gpu_count": Param(default=1, enum=INSTANCES_TYPES["gpu"]["count"]),
            "instance_name": Param(
                default=gce_params["instance_name"]
                + "-"
                + train_params["config_file_name"],
                type="string",
            ),
            "run_name": Param(
                default=train_params["run_name"], type=["string", "null"]
            ),
            "experiment_name": Param(
                default=train_params["experiment_name"], type=["string", "null"]
            ),
            "upload_embeddings": Param(
                default=True,
                type="boolean",
                description="Whether to upload embeddings to BigQuery after training",
            ),
            "evaluate_on_dummy": Param(
                default=False,
                type="boolean",
                description="Whether to evaluate the model on dummy data",
            ),
        },
    ) as dag
):
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        gpu_count="{{ params.gpu_count }}",
        gpu_type="{{ params.gpu_type }}",
        retries=2,
        labels={"job_type": "long_ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="'3.10'",
        base_dir=dag_config["BASE_DIR"],
        retries=2,
    )

    # TODO: Refacto this part to do that in Python (or at least the train/val/test split)
    with TaskGroup(group_id="split_train_val_test") as split_train_val_test:
        split_tasks = {}
        for dataset in ["training", "validation", "test"]:
            # The params.input_type tells the .sql files which table to take as input
            split_tasks[dataset] = BigQueryInsertJobOperator(
                project_id=GCP_PROJECT_ID,
                task_id=f"create_{dataset}_table",
                configuration={
                    "query": {
                        "query": (
                            IMPORT_TRAINING_SQL_PATH
                            / f"recommendation_{dataset}_data.sql"
                        ).as_posix(),
                        "useLegacySql": False,
                        "destinationTable": {
                            "projectId": GCP_PROJECT_ID,
                            "datasetId": BIGQUERY_TMP_DATASET,
                            "tableId": f"{DATE}_recommendation_{dataset}_data",
                        },
                        "writeDisposition": "WRITE_TRUNCATE",
                    }
                },
                dag=dag,
            )

        split_tasks["training"] >> split_tasks["validation"] >> split_tasks["test"]

    with TaskGroup(group_id="import_tables_to_bucket") as import_tables:
        import_tasks = {}
        for split in ["training", "validation", "test"]:
            import_tasks[split] = BigQueryInsertJobOperator(
                project_id=GCP_PROJECT_ID,
                task_id=f"store_{split}_data",
                configuration={
                    "extract": {
                        "sourceTable": {
                            "projectId": GCP_PROJECT_ID,
                            "datasetId": BIGQUERY_TMP_DATASET,
                            "tableId": f"{DATE}_recommendation_{split}_data",
                        },
                        "compression": None,
                        "destinationUris": f"{dag_config['TEMP_PATH']}/raw_recommendation_{split}_data/data-*.parquet",
                        "destinationFormat": "PARQUET",
                    }
                },
                dag=dag,
            )

        import_tasks["booking"] = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_booking_to_bucket",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_ML_RECOMMENDATION_DATASET,
                        "tableId": "training_data_booking",
                    },
                    "compression": None,
                    "destinationUris": f"{dag_config['TEMP_PATH']}/bookings/data-*.parquet",
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

    with TaskGroup(group_id="preprocess_data") as preprocess_data:
        preprocess_data_tasks = {}
        for split in ["training", "validation", "test"]:
            preprocess_data_tasks[split] = SSHGCEOperator(
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
        (
            preprocess_data_tasks["training"]
            >> preprocess_data_tasks["validation"]
            >> preprocess_data_tasks["test"]
        )

    train = SSHGCEOperator(
        task_id="train",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/train.py "
        "--config-file-name {{ params.config_file_name }} "
        "--experiment-name {{ params.experiment_name }} "
        "--batch-size {{ params.batch_size }} "
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
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/evaluate.py "
        "--experiment-name {{ params.experiment_name }} "
        "--dummy {{ params.evaluate_on_dummy }} ",
        dag=dag,
    )

    branch_upload_embeddings = BranchPythonOperator(
        task_id="branch_upload_embeddings",
        python_callable=should_upload_embeddings,
        provide_context=True,
        dag=dag,
    )

    skip_upload_embeddings = DummyOperator(
        task_id="skip_upload_embeddings",
        dag=dag,
    )

    upload_embeddings = SSHGCEOperator(
        task_id="upload_embeddings",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        environment=dag_config,
        command=f"PYTHONPATH=. python {dag_config['MODEL_DIR']}/upload_embeddings_to_bq.py "
        "--experiment-name {{ params.experiment_name }} "
        "--run-name {{ params.run_name }} "
        f"--dataset-id {BIGQUERY_ML_PREPROCESSING_DATASET}",
        dag=dag,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="none_failed",
    )

    send_slack_notif_success = SendSlackMessageOperator(
        task_id="send_slack_notif_success",
        webhook_token=SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN,
        trigger_rule="none_failed",
        block=create_algo_training_slack_block(
            dag_config["MODEL_DIR"], MLFLOW_URL, ENV_SHORT_NAME
        ),
    )

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> preprocess_data
        >> train
        >> evaluate
        >> branch_upload_embeddings
        >> [upload_embeddings, skip_upload_embeddings]
        >> gce_instance_stop
        >> send_slack_notif_success
    )
    (start >> split_train_val_test >> import_tables >> preprocess_data)
