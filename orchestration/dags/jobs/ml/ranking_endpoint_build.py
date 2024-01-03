from airflow import DAG
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH
from airflow.models import Param
from datetime import datetime, timedelta
from common import macros
from common.alerts import task_fail_slack_alert
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from common.config import ENV_SHORT_NAME, DAG_FOLDER, BIGQUERY_TMP_DATASET
from common.utils import get_airflow_schedule

default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

DEFAULT_REGION = "europe-west1"
BASE_DIR = "data-gcp/jobs/ml_jobs/ranking_endpoint"
gce_params = {
    "instance_name": f"ranking-endpoint-build-{ENV_SHORT_NAME}",
    "experiment_name": f"ranking_endpoint_v1.1_{ENV_SHORT_NAME}",
    "model_name": f"v0.0_{ENV_SHORT_NAME}",
    "run_name": "default",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-2",
        "prod": "n1-standard-8",
    },
}
schedule_dict = {"prod": "0 20 * * 5", "dev": "0 20 * * *", "stg": "0 20 * * 3"}


with DAG(
    "ranking_endpoint_build",
    default_args=default_args,
    description="Train and build Ranking Endpoint",
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
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"],
            type="string",
        ),
        "experiment_name": Param(default=gce_params["experiment_name"], type="string"),
        "run_name": Param(default=gce_params["run_name"], type="string"),
        "model_name": Param(default=gce_params["model_name"], type="string"),
        "table_name": Param(default="training_ranking_data", type="string"),
        "dataset_name": Param(default=BIGQUERY_TMP_DATASET, type="string"),
    },
) as dag:
    import_table = BigQueryExecuteQueryOperator(
        task_id=f"create_train_ranking_table",
        sql=(
            IMPORT_TRAINING_SQL_PATH / f"ranking_endpoint_training_data.sql"
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table="{{ params.dataset_name}}.{{ params.table_name }}",
        dag=dag,
    )
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "ml"},
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name="{{ params.instance_name }}",
        command="{{ params.branch }}",
        python_version="3.10",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
        dag=dag,
        retries=2,
    )

    deploy_model = SSHGCEOperator(
        task_id="containerize_model",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="python deploy_model.py "
        "--experiment-name {{ params.experiment_name }} "
        "--run-name {{ params.run_name }} "
        "--model-name {{ params.model_name }} "
        "--dataset-name {{ params.dataset_name }} "
        "--table-name {{ params.table_name }}",
        dag=dag,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    (
        gce_instance_start
        >> import_table
        >> fetch_code
        >> install_dependencies
        >> deploy_model
        >> gce_instance_stop
    )
