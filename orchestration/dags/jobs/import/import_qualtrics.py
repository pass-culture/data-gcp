import datetime
from airflow import DAG
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)

from common.config import DAG_FOLDER
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID

from common.utils import get_airflow_schedule

from common.alerts import task_fail_slack_alert

from common import macros


GCE_INSTANCE = f"import-qualtrics-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/qualtrics"
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
    "import_qualtrics",
    default_args=default_dag_args,
    description="Import qualtrics tables",
    schedule_interval=get_airflow_schedule(
        "0 0 * * 1"
    ),  # execute each Monday at midnight
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

    import_opt_out_to_bigquery = SSHGCEOperator(
        task_id="import_opt_out_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --task import_opt_out_users",
        do_xcom_push=True,
    )

    import_ir_answers_to_bigquery = SSHGCEOperator(
        task_id="import_ir_answers_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --task import_ir_survey_answers",
        do_xcom_push=True,
    )

    import_all_answers_to_bigquery = SSHGCEOperator(
        task_id="import_all_answers_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --task import_all_survey_answers",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )
    (gce_instance_start >> fetch_code >> install_dependencies)
    (install_dependencies >> import_opt_out_to_bigquery >> gce_instance_stop)
    (install_dependencies >> import_ir_answers_to_bigquery >> gce_instance_stop)
    (install_dependencies >> import_all_answers_to_bigquery >> gce_instance_stop)
