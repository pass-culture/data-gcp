import datetime
from airflow import DAG
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
)
from common.utils import get_airflow_schedule
from common.alerts import task_fail_slack_alert
from common import macros
from common.utils import get_airflow_schedule

GCE_INSTANCE = f"import-tiktok-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/tiktok"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    "import_tiktok",
    default_args=default_dag_args,
    description="Import Tiktok Organic Data",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    dagrun_timeout=datetime.timedelta(minutes=120),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "n_days": Param(
            default=-14,
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
        python_version="3.10",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

    tiktok_to_bq = SSHGCEOperator(
        task_id="tiktok_to_bq",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python main.py --start-date {{ add_days(ds, params.days) }} --end-date {{ ds }} ",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> tiktok_to_bq
        >> gce_instance_stop
    )
