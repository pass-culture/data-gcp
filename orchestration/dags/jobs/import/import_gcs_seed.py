import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
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
from common.utils import (
    depends_loop,
    get_airflow_schedule,
)
from dependencies.gcs_seed.import_gcs_seed import ANALYTICS_TABLES

MICROSERVICE_PATH = "jobs/etl_jobs/internal/gcs_seed"
DAG_NAME = "import_gcs_seed"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import seed data from GCS to BQ",
    schedule=get_airflow_schedule("00 01 * * *"),
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    import_seed_data_op = CustomKubernetesPodOperator(
        task_id="import_seed_data_op",
        orchestration_mode="celery",
        queue="k8s-watcher",
        runtime_mode="gitsynced",
        runtime_branch="{{ params.branch }}",
        runtime_image="py313",
        runtime_image_tag="v1",
        microservice_path=MICROSERVICE_PATH,
        arguments=["main.py"],
        container_resources=DEFAULT_CONTAINER_RESOURCES,
        env_vars={"PROJECT_NAME": GCP_PROJECT_ID},
    )

    end_raw = EmptyOperator(task_id="end_raw", dag=dag)

    analytics_table_jobs = {}
    for name, params in ANALYTICS_TABLES.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)

        analytics_table_jobs[name] = {
            "operator": task,
            "depends": params.get("depends", []),
            "dag_depends": params.get("dag_depends", []),
        }

    end = EmptyOperator(task_id="end", dag=dag)
    analytics_table_tasks = depends_loop(
        ANALYTICS_TABLES,
        analytics_table_jobs,
        end_raw,
        dag=dag,
        default_end_operator=end,
    )

    (start >> import_seed_data_op >> end_raw >> analytics_table_tasks)
