import datetime
from itertools import chain

from common import macros
from common.alerts import on_failure_combined_callback
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    INSTANCES_TYPES,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param

DAG_NAME = "import_zendesk"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/zendesk"

dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": on_failure_combined_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Zendesk data into BigQuery",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    render_template_as_native_obj=True,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "ndays": Param(
            default=7 if ENV_SHORT_NAME == "prod" else 1,
            type="integer",
        ),
        "job": Param(
            default="both",
            enum=["macro_stat", "ticket_stat", "both"],
            type="string",
            help="Specify the job to run: 'macro_stat', 'ticket_stat', or 'both'.",
        ),
        "prior_date": Param(
            default=None,
            type="string",
            help="Optional prior date (YYYY-MM-DD) to calculate the ndays range from instead of now().",
        ),
        "instance_name": Param(
            default=f"import-zendesk-{ENV_SHORT_NAME}", type="string"
        ),
        "instance_type": Param(
            default="n1-standard-2",
            enum=list(chain(*INSTANCES_TYPES["cpu"].values())),
        ),
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
        dag=dag,
        retries=2,
    )

    import_data_to_bigquery = SSHGCEOperator(
        task_id="import_to_bigquery",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --ndays {{ params.ndays }} --job {{ params.job }} {% if params.prior_date %} --prior_date {{ params.prior_date }} {% endif %}",
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    (
        gce_instance_start
        >> fetch_install_code
        >> import_data_to_bigquery
        >> gce_instance_stop
    )
