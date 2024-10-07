import datetime

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, ENV_SHORT_NAME, GCP_PROJECT_ID
from common.operators.bigquery import bigquery_job_task
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import (
    depends_loop,
    get_airflow_schedule,
)
from dependencies.gcs_seed.import_gcs_seed import ANALYTICS_TABLES

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

GCE_INSTANCE = f"import-gcs-seed-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/gcs_seed"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

with DAG(
    "import_gcs_seed",
    default_args=default_dag_args,
    description="Import seed data from GCS to BQ",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.9",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

    import_seed_data_op = SSHGCEOperator(
        task_id="import_seed_data_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    end_raw = DummyOperator(task_id="end_raw", dag=dag)

    analytics_table_jobs = {}
    for name, params in ANALYTICS_TABLES.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)

        analytics_table_jobs[name] = {
            "operator": task,
            "depends": params.get("depends", []),
            "dag_depends": params.get("dag_depends", []),
        }

    end = DummyOperator(task_id="end", dag=dag)
    analytics_table_tasks = depends_loop(
        ANALYTICS_TABLES,
        analytics_table_jobs,
        end_raw,
        dag=dag,
        default_end_operator=end,
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> import_seed_data_op
        >> gce_instance_stop
        >> end_raw
        >> analytics_table_tasks
    )
