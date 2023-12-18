import datetime
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)

from dependencies.contentful.import_contentful import contentful_tables


from common.utils import (
    depends_loop,
    getting_service_account_token,
    get_airflow_schedule,
)
from common.operators.biquery import bigquery_job_task
from common.alerts import task_fail_slack_alert

from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER

GCE_INSTANCE = f"import-contentful-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/contentful"
dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    "import_contentful",
    default_args=default_dag_args,
    description="Import contentful tables",
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
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.8",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

    import_contentful_data_to_bigquery = SSHGCEOperator(
        task_id="import_contentful_data_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

start = DummyOperator(task_id="start", dag=dag)

table_jobs = {}
for table, job_params in contentful_tables.items():
    task = bigquery_job_task(dag, table, job_params)
    table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
        "dag_depends": job_params.get("dag_depends", []),
    }

end = DummyOperator(task_id="end", dag=dag)
table_jobs = depends_loop(
    contentful_tables, table_jobs, start, dag=dag, default_end_operator=end
)

(
    start
    >> gce_instance_start
    >> fetch_code
    >> install_dependencies
    >> import_contentful_data_to_bigquery
    >> gce_instance_stop
    >> table_jobs
)
