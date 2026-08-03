from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_job_task
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import depends_loop, get_airflow_schedule
from dependencies.batch.import_batch import import_batch_tables

MICROSERVICE_PATH = "jobs/etl_jobs/external/batch"
DAG_NAME = "import_batch"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Import batch push notifications statistics",
    schedule=get_airflow_schedule("0 0 * * *"),  # import every day at 00:00
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    start = EmptyOperator(task_id="start")

    _kpo_common = {
        "orchestration_mode": "celery",
        "queue": "k8s-watcher",
        "runtime_mode": "gitsynced",
        "runtime_branch": "{{ params.branch }}",
        "runtime_image": "py313",
        "runtime_image_tag": "v1",
        "microservice_path": MICROSERVICE_PATH,
        "container_resources": DEFAULT_CONTAINER_RESOURCES,
        "retries": 2,
    }

    import_ios = CustomKubernetesPodOperator(
        task_id="import_ios",
        arguments=["main.py", GCP_PROJECT_ID, ENV_SHORT_NAME, "ios"],
        **_kpo_common,
    )

    import_android = CustomKubernetesPodOperator(
        task_id="import_android",
        arguments=["main.py", GCP_PROJECT_ID, ENV_SHORT_NAME, "android"],
        **_kpo_common,
    )

    start_analytics_table_tasks = EmptyOperator(
        task_id="start_analytics_tasks", dag=dag
    )

    analytics_table_jobs = {}
    for table, job_params in import_batch_tables.items():
        job_params["destination_table"] = job_params.get("destination_table", table)
        task = bigquery_job_task(dag=dag, table=table, job_params=job_params)
        analytics_table_jobs[table] = {
            "operator": task,
            "depends": job_params.get("depends", []),
            "dag_depends": job_params.get("dag_depends", []),
        }

    end = EmptyOperator(task_id="end", dag=dag)

    analytics_table_tasks = depends_loop(
        import_batch_tables,
        analytics_table_jobs,
        start_analytics_table_tasks,
        dag,
        default_end_operator=end,
    )

    start >> [import_ios, import_android] >> start_analytics_table_tasks
