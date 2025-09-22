import datetime
import os

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    DATA_GCS_BUCKET_NAME,
)

from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_vm_callback,
}

dag_id = "export_ppg"

GCE_INSTANCE = f"export-ppg-{dag_id}-{ENV_SHORT_NAME }"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/ppg"
GCP_STORAGE_URI = "https://storage.googleapis.com"
DBT_SQL_PATH = "dags/data_gcp_dbt/mart/external_reporting"
AWAITED_TABLES = [""]
print(DBT_SQL_PATH)
if main := "__main__":
    for root, _, files in os.walk(DBT_SQL_PATH):
        print(root)
        for f in files:
            if f.endswith(".sql"):
                sql_path = os.path.join(root, f)
                task_id = f"run_{f.replace('.sql','')}"
                print(f"{task_id}: {sql_path}")


DATE = "{{ yyyymmdd(ds) }}"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}


with DAG(
    dag_id,
    default_args=default_dag_args,
    description="Data reporting export for ministère & DRAC",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[dag_id].get(ENV_SHORT_NAME)),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DE.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-4",
            type="string",
        ),
        "instance_name": Param(
            default=GCE_INSTANCE,
            type="string",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("waiting_group") as waiting_group:
        # for table_name in AWAITED_TABLES:
        #     wait_operator = delayed_waiting_operator(
        #         dag=dag,
        #         external_dag_id="dbt_run_dag",
        #         task_id=f"data_transformation.{table_name}",
        #         timeout=600,
        # )

        for root, _, files in os.walk(DBT_SQL_PATH):
            for f in files:
                if f.endswith(".sql"):
                    sql_path = os.path.join(root, f)
                    task_id = f"run_{f.replace('.sql','')}"

                    EmptyOperator(
                        task_id=task_id,
                    )

    # gce_instance_start = StartGCEOperator(
    #     instance_name=GCE_INSTANCE,
    #     task_id="gce_start_task",
    #     labels={"dag_name": dag_id},
    # )

    # fetch_install_code = InstallDependenciesOperator(
    #     task_id="fetch_install_code",
    #     instance_name=GCE_INSTANCE,
    #     branch="{{ params.branch }}",
    #     python_version="3.10",
    #     base_dir=BASE_PATH,
    # )

    # gce_generate_reports = SSHGCEOperator(
    #     task_id="gce_generate_reports",
    #     instance_name=GCE_INSTANCE,
    #     base_dir=BASE_PATH,
    #     command="python main.py "
    # )

    # gce_export_reports = SSHGCEOperator(
    #     task_id="gce_export_reports",
    #     instance_name=GCE_INSTANCE,
    #     base_dir=BASE_PATH,
    #     command="python export_gcs.py ",
    # )

    # gce_export_reports = EmptyOperator(
    #     task_id="gce_export_reports_EMPTY_TASK"
    # )

    # gce_instance_stop = DeleteGCEOperator(
    #     task_id="gce_stop_task", instance_name=GCE_INSTANCE
    # )

    (
        start >> waiting_group
    )  # >> gce_instance_start >> fetch_install_code >> gce_generate_reports >> gce_export_reports >> gce_instance_stop >> end
