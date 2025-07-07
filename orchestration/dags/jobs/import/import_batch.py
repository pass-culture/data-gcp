from datetime import datetime, timedelta

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_job_task
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import depends_loop, get_airflow_schedule
from dependencies.batch.import_batch import import_batch_tables

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

GCE_INSTANCE = f"import-batch-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/batch"
DAG_NAME = "import_batch"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Import batch push notifications statistics",
    schedule_interval=get_airflow_schedule("0 0 * * *"),  # import every day at 00:00
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    start = DummyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        instance_type="n1-standard-1",
        task_id="gce_start_task",
        retries=2,
        preemptible=False,
        labels={"job_type": "long_task", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
        dag=dag,
        retries=2,
    )

    ios_job = SSHGCEOperator(
        task_id="import_ios",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        python main.py {GCP_PROJECT_ID} {ENV_SHORT_NAME} ios
        """,
        retries=2,
    )

    android_job = SSHGCEOperator(
        task_id="import_android",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        python main.py {GCP_PROJECT_ID} {ENV_SHORT_NAME} android
        """,
        retries=2,
    )

    gce_instance_stop = DeleteGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task"
    )

    start_analytics_table_tasks = DummyOperator(
        task_id="start_analytics_tasks", dag=dag
    )

    analytics_table_jobs = {}
    for table, job_params in import_batch_tables.items():
        job_params["destination_table"] = job_params.get("destination_table", table)
        task = bigquery_job_task(dag=dag, table=table, job_params=job_params)
        analytics_table_jobs[table] = {
            "operator": task,
            "depends": job_params.get("depends", []),
            "dag_depends": job_params.get("dag_depends", []),  # liste de dag_id
        }

    end = DummyOperator(task_id="end", dag=dag)

    analytics_table_tasks = depends_loop(
        import_batch_tables,
        analytics_table_jobs,
        start_analytics_table_tasks,
        dag,
        default_end_operator=end,
    )

    (start >> gce_instance_start >> fetch_install_code)
    (
        fetch_install_code
        >> ios_job
        >> android_job
        >> gce_instance_stop
        >> start_analytics_table_tasks
        >> analytics_table_tasks
    )
