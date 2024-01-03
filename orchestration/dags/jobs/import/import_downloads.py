import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.operators.biquery import bigquery_job_task

from common.config import DAG_FOLDER

from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    DAG_FOLDER,
)

from common.utils import getting_service_account_token, get_airflow_schedule

from common.alerts import task_fail_slack_alert

from common import macros

from dependencies.downloads.import_downloads import ANALYTICS_TABLES

GCE_INSTANCE = f"import-downloads-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/downloads"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
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
    "import_downloads",
    default_args=default_dag_args,
    description="Import downloads tables",
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

    import_downloads_data_to_bigquery = SSHGCEOperator(
        task_id="import_downloads_data_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )
# only downloads included here
analytics_tasks = []
for table, params in ANALYTICS_TABLES.items():
    task = bigquery_job_task(table=table, dag=dag, job_params=params)
    analytics_tasks.append(task)

end = DummyOperator(task_id="end", dag=dag)
(
    gce_instance_start
    >> fetch_code
    >> install_dependencies
    >> import_downloads_data_to_bigquery
    >> gce_instance_stop
    >> analytics_tasks
    >> end
)
