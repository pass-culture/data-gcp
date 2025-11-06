import datetime

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
from dependencies.appsflyer.import_appsflyer import dag_tables

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

GCE_INSTANCE = f"import-appsflyer-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/appsflyer"
DAG_NAME = "import_appsflyer"

GCS_ETL_PARAMS = {
    "DATE": "{{ ds }}",
    "GCS_BASE_PATH": "af-xpend-cost-etl-acc-pfpgdnfn-appsflyer-data-prod/cost_etl/v1/dt={{ ds }}/b=4",
    "PREFIX_TABLE_NAME": "appsflyer_cost",
}

default_dag_args = {
    "start_date": datetime.datetime(2022, 1, 1),
    "retries": 1,
    "on_failure_callback": on_failure_vm_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}
schedule_dict = {"prod": "30 01 * * *", "dev": None, "stg": None}

with DAG(
    DAG_NAME,
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.9",
        base_dir=BASE_PATH,
        retries=2,
    )

    activity_report_op = SSHGCEOperator(
        task_id="activity_report_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python api_import.py --n-days {{ params.n_days }} --table-name activity_report ",
    )

    daily_report_op = SSHGCEOperator(
        task_id="daily_report_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python api_import.py --n-days {{ params.n_days }} --table-name daily_report ",
    )

    partner_report_op = SSHGCEOperator(
        task_id="partner_report_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python api_import.py --n-days {{ params.n_days }} --table-name partner_report ",
    )

    in_app_event_report_op = SSHGCEOperator(
        task_id="in_app_event_report_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python api_import.py --n-days {{ params.n_days }} --table-name in_app_event_report ",
    )

    gcs_cost_etl_op = (
        SSHGCEOperator(
            task_id="gcs_cost_etl_op",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_PATH,
            command=f"python gcs_import.py --gcs-base-path {GCS_ETL_PARAMS['GCS_BASE_PATH']} --prefix-table-name {GCS_ETL_PARAMS['PREFIX_TABLE_NAME']} --date {GCS_ETL_PARAMS['DATE']} ",
        )
        if ENV_SHORT_NAME == "prod"
        else DummyOperator(task_id="skip_gcs_cost_etl_op", dag=dag)
    )

    gce_instance_stop = DeleteGCEOperator(
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
    >> fetch_install_code
    >> activity_report_op
    >> daily_report_op
    >> partner_report_op
    >> in_app_event_report_op
    >> gcs_cost_etl_op
    >> gce_instance_stop
    >> start
)
