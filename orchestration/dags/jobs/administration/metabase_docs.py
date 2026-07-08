import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

GCE_INSTANCE = f"metabase-docs-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/metabase-docs"
DAG_NAME = "metabase_docs"
dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 2,
    "on_failure_callback": on_failure_vm_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description=(
        "Enrich + reconcile Notion dashboard docs into "
        "int_metabase_<env>.dashboard_documentation"
    ),
    # After import_notion + metabase_governance (08:00) and the daily dbt refresh of asset_catalog.
    schedule=get_airflow_schedule("00 11 * * *") if ENV_SHORT_NAME == "prod" else None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_name": Param(default=GCE_INSTANCE, type="string"),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        instance_type="n1-standard-2",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.13",
        base_dir=BASE_PATH,
        dag=dag,
        retries=2,
    )

    enrich_dashboard_docs = SSHGCEOperator(
        task_id="enrich_dashboard_docs",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py enrich",
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

(gce_instance_start >> fetch_install_code >> enrich_dashboard_docs >> gce_instance_stop)
