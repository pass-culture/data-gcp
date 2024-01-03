import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from dependencies.appsflyer.import_appsflyer import dag_tables
from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task
from common.utils import depends_loop, get_airflow_schedule
from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER

from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)

GCE_INSTANCE = f"import-appsflyer-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/appsflyer"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2022, 1, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}
schedule_dict = {"prod": "00 01 * * *", "dev": None, "stg": "00 02 * * *"}

with DAG(
    "import_appsflyer",
    default_args=default_dag_args,
    description="Import Appsflyer tables",
    schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "n_days": Param(
            default=14 if ENV_SHORT_NAME == "prod" else 3,
            type="integer",
        ),
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

    activity_report_op = SSHGCEOperator(
        task_id="activity_report_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --n-days {{ params.n_days }} --table-name activity_report ",
    )

    daily_report_op = SSHGCEOperator(
        task_id="daily_report_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --n-days {{ params.n_days }} --table-name daily_report ",
    )

    in_app_event_report_op = SSHGCEOperator(
        task_id="in_app_event_report_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --n-days {{ params.n_days }} --table-name in_app_event_report ",
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )


start = DummyOperator(task_id="start", dag=dag)

table_jobs = {}
for table, job_params in dag_tables.items():
    task = bigquery_job_task(dag, table, job_params)
    table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
        "dag_depends": job_params.get("dag_depends", []),
    }

end = DummyOperator(task_id="end", dag=dag)
table_jobs = depends_loop(
    dag_tables, table_jobs, start, dag=dag, default_end_operator=end
)

(
    gce_instance_start
    >> fetch_code
    >> install_dependencies
    >> activity_report_op
    >> daily_report_op
    >> in_app_event_report_op
    >> gce_instance_stop
    >> start
)
table_jobs
