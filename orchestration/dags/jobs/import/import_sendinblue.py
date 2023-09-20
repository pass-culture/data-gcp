import datetime
import json
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
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    DAG_FOLDER,
)
from common.operators.biquery import bigquery_job_task
from common.utils import (
    getting_service_account_token,
    depends_loop,
    get_airflow_schedule,
)

from common.alerts import task_fail_slack_alert

from common import macros

from dependencies.sendinblue.import_sendinblue import analytics_tables


GCE_INSTANCE = f"import-sendinblue-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/sendinblue"
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
    "import_sendinblue",
    default_args=default_dag_args,
    description="Import sendinblue tables",
    schedule_interval=get_airflow_schedule("00 04 * * *")
    if ENV_SHORT_NAME in ["prod", "stg"]
    else get_airflow_schedule("00 07 * * *"),
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

    import_transactional_data_to_raw = SSHGCEOperator(
        task_id="import_transactional_data_to_raw",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --target transactional ",
        do_xcom_push=True,
    )

    import_newsletter_data_to_raw = SSHGCEOperator(
        task_id="import_newsletter_data_to_raw",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --target newsletter ",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    end_raw = DummyOperator(task_id="end_raw", dag=dag)

    analytics_table_jobs = {}
    for name, params in analytics_tables.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)
        analytics_table_jobs[name] = {
            "operator": task,
            "depends": params.get("depends", []),
            "dag_depends": params.get("dag_depends", []),
        }

        # import_tables_to_analytics_tasks.append(task)

    analytics_table_tasks = depends_loop(analytics_table_jobs, end_raw, dag=dag)

    (
        gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> import_newsletter_data_to_raw
        >> import_transactional_data_to_raw
        >> gce_instance_stop
        >> end_raw
        >> analytics_table_tasks
    )
