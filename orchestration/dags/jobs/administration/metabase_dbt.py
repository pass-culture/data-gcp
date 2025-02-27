import datetime

from common import macros
from common.alerts import on_failure_combined_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    GCS_COMPOSER_BUCKET,
    VM_AIRFLOW_DAG_TAG,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import (
    get_airflow_schedule,
)

from airflow import DAG
from airflow.models import Param

DAG_NAME = "metabase-dbt"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/metabase-dbt"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 0,
    "on_failure_callback": on_failure_combined_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import metabase tables from CloudSQL & archive old cards",
    schedule_interval=get_airflow_schedule("0 */6 * * 1-5")
    if ENV_SHORT_NAME == "prod"
    else None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_name": Param(
            default=f"metabase-dbt-{ENV_SHORT_NAME}",
            type="string",
        ),
        "composer_bucket_name": Param(
            default=GCS_COMPOSER_BUCKET,
            type="string",
        ),
        "exposure_dataset_name": Param(
            default=f"int_metabase_{ENV_SHORT_NAME}",
            type="string",
        ),
        "exposure_table_name": Param(
            default="internal_dbt_exposure",
            type="string",
        ),
    },
    tags=[VM_AIRFLOW_DAG_TAG, DAG_TAGS.DE.value],
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name="{{ params.instance_name }}",
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )
    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        base_dir=BASE_PATH,
        retries=2,
        dag=dag,
    )

    export_models_to_metabase = SSHGCEOperator(
        task_id="export_models_to_metabase",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py export-models --composer-bucket-name {{ params.composer_bucket_name}} ",
        do_xcom_push=True,
    )

    export_exposures_to_airflow = SSHGCEOperator(
        task_id="export_exposures_to_airflow",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py export-exposures --composer-bucket-name {{ params.composer_bucket_name}} --exposure-dataset-name {{ params.exposure_dataset_name }} --exposure-table-name {{ params.exposure_table_name }}",
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    (
        gce_instance_start
        >> fetch_install_code
        >> export_models_to_metabase
        >> export_exposures_to_airflow
        >> gce_instance_stop
    )
