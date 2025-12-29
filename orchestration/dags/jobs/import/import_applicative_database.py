import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_base_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_federated_query_task, bigquery_job_task
from common.utils import get_airflow_schedule
from dependencies.applicative_database.import_applicative_database import (
    HISTORICAL_CLEAN_APPLICATIVE_TABLES,
    PARALLEL_TABLES,
    SEQUENTIAL_TABLES,
)

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 6,
    "on_failure_callback": on_failure_base_callback,
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
    tags=[DAG_TAGS.DE.value],
)

start = EmptyOperator(task_id="start", dag=dag)

# Sequential table import tasks
with TaskGroup(group_id="sequential_tasks_group", dag=dag) as sequential_tasks_group:
    seq_tasks = []
    for table, params in SEQUENTIAL_TABLES.items():
        task = bigquery_federated_query_task(
            dag, task_id=f"import_sequential_to_raw_{table}", job_params=params
        )
        seq_tasks.append(task)

seq_end = EmptyOperator(task_id="seq_end", dag=dag)

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

parallel_end = EmptyOperator(task_id="parallel_end", dag=dag)

# Historical table import tasks
with TaskGroup(
    group_id="historical_applicative_group", dag=dag
) as historical_applicative_group:
    historical_tasks = []
    for table, params in HISTORICAL_CLEAN_APPLICATIVE_TABLES.items():
        task = bigquery_job_task(dag, table, params)
        historical_tasks.append(task)

end = EmptyOperator(task_id="end", dag=dag)


(
    start
    >> sequential_tasks_group
    >> seq_end
    >> raw_parallel_operations_group
    >> parallel_end
    >> historical_applicative_group
    >> end
)
