import datetime

from common import macros
from common.config import (
    BIGQUERY_INT_RAW_DATASET,
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
    get_tables_config_dict,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

GCE_INSTANCE = f"historize-applicative-database-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/export_applicative"

SNAPSHOT_MODELS_PATH = "data_gcp_dbt/models/intermediate/raw"
SNAPSHOT_TABLES = get_tables_config_dict(
    PATH=DAG_FOLDER + "/" + SNAPSHOT_MODELS_PATH,
    BQ_DATASET=BIGQUERY_INT_RAW_DATASET,
    is_source=True,
    dbt_alias=True,
)

dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 6,
    # "on_failure_callback": task_fail_slack_alert,
    "on_failure_callback": None,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}


dag = DAG(
    "historize_applicative_database",
    default_args=default_dag_args,
    description="historize applicative database",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=480),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=["VM"],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
    },
)

start = DummyOperator(task_id="start", dag=dag)

with TaskGroup(group_id="wait_transfo", dag=dag) as wait:
    for table_name in SNAPSHOT_TABLES.keys():
        waiting_task = delayed_waiting_operator(
            dag,
            external_dag_id="dbt_run_dag",
            external_task_id=f"data_transformation.{table_name}",
        )


gce_instance_start = StartGCEOperator(
    instance_name=GCE_INSTANCE,
    task_id="gce_start_task",
    dag=dag,
    preemptible=False,
)
fetch_install_code = InstallDependenciesOperator(
    task_id="fetch_install_code",
    instance_name=GCE_INSTANCE,
    branch="{{ params.branch }}",
    installer="uv",
    python_version="3.10",
    base_dir=BASE_PATH,
    dag=dag,
)
with TaskGroup(group_id="applicatives_to_gcs", dag=dag) as tables_to_gcs:
    for table_name, bq_config in SNAPSHOT_TABLES.items():
        alias = bq_config["table_alias"]
        dataset = bq_config["source_dataset"]
        table_to_gcs = SSHGCEOperator(
            task_id=f"snapshot_to_gcs_{table_name}",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_PATH,
            environment=dag_config,
            command=f"python main.py --table-name {alias} --table-dataset {dataset} --project-id {GCP_PROJECT_ID} --gcs-bucket {DATA_GCS_BUCKET_NAME}",
            do_xcom_push=False,
            installer="uv",
            dag=dag,
        )


gce_instance_stop = StopGCEOperator(
    task_id="gce_stop_task",
    instance_name=GCE_INSTANCE,
    dag=dag,
)

(
    start
    >> wait
    >> gce_instance_start
    >> fetch_install_code
    >> tables_to_gcs
    >> gce_instance_stop
)
