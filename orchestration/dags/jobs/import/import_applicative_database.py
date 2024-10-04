import datetime

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.biquery import bigquery_federated_query_task, bigquery_job_task
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule
from dependencies.applicative_database.import_applicative_database import (
    HISTORICAL_CLEAN_APPLICATIVE_TABLES,
    PARALLEL_TABLES,
    SEQUENTIAL_TABLES,
)

from airflow import DAG
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

GCE_INSTANCE = f"import-applicative-database-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/export_applicative"

dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 6,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_applicative_database",
    default_args=default_dag_args,
    description="Import tables from CloudSQL and enrich data for create dashboards with Metabase",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=480),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
    },
)

start = DummyOperator(task_id="start", dag=dag)

# Sequential table import tasks
with TaskGroup(group_id="sequential_tasks_group", dag=dag) as sequential_tasks_group:
    seq_tasks = []
    for table, params in SEQUENTIAL_TABLES.items():
        task = bigquery_federated_query_task(
            dag, task_id=f"import_sequential_to_raw_{table}", job_params=params
        )
        seq_tasks.append(task)

seq_end = DummyOperator(task_id="seq_end", dag=dag)

chain(*seq_tasks, seq_end)

# Parallel table import tasks
with TaskGroup(
    group_id="raw_parallel_operations_group", dag=dag
) as raw_parallel_operations_group:
    parallel_tasks = []
    for table, params in PARALLEL_TABLES.items():
        task = bigquery_federated_query_task(
            dag, task_id=f"import_parallel_to_raw_{table}", job_params=params
        )
        parallel_tasks.append(task)

parallel_end = DummyOperator(task_id="parallel_end", dag=dag)

# Historical table import tasks
with TaskGroup(
    group_id="historical_applicative_group", dag=dag
) as historical_applicative_group:
    historical_tasks = []
    for table, params in HISTORICAL_CLEAN_APPLICATIVE_TABLES.items():
        task = bigquery_job_task(dag, table, params)
        historical_tasks.append(task)

end = DummyOperator(task_id="end", dag=dag)

# Historization tasks
gce_instance_start = StartGCEOperator(
    instance_name=GCE_INSTANCE,
    task_id="gce_start_task",
    dag=dag,
)

fetch_install_code = InstallDependenciesOperator(
    task_id="fetch_install_code",
    instance_name=GCE_INSTANCE,
    branch="{{ params.branch }}",
    installer="uv",
    python_version="3.10",
    base_dir=BASE_PATH,
)

applicative_to_gcs = SSHGCEOperator(
    task_id="applicative_to_gcs",
    instance_name=GCE_INSTANCE,
    base_dir=BASE_PATH,
    environment=dag_config,
    command="python main.py ",
    installer="uv",
)

gce_instance_stop = StopGCEOperator(task_id="gce_stop_task", instance_name=GCE_INSTANCE)

(
    start
    >> sequential_tasks_group
    >> seq_end
    >> raw_parallel_operations_group
    >> parallel_end
    >> (
        historical_applicative_group,
        (
            gce_instance_start
            >> fetch_install_code
            >> applicative_to_gcs
            >> gce_instance_stop
        ),
    )
    >> end
)
