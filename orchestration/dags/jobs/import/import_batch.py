from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, DAG_FOLDER
from common.utils import get_airflow_schedule, depends_loop
from dependencies.batch.import_batch import import_batch_tables
from common.operators.biquery import bigquery_job_task
from common import macros


GCE_INSTANCE = f"import-batch-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/batch"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "import_batch",
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
) as dag:
    start = DummyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        retries=2,
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.10",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
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
    )

    android_job = SSHGCEOperator(
        task_id="import_android",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command=f"""
        python main.py {GCP_PROJECT_ID} {ENV_SHORT_NAME} android      
        """,
    )

    gce_instance_stop = StopGCEOperator(
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

    (start >> gce_instance_start >> fetch_code >> install_dependencies)
    (
        install_dependencies
        >> ios_job
        >> android_job
        >> gce_instance_stop
        >> start_analytics_table_tasks
        >> analytics_table_tasks
    )
