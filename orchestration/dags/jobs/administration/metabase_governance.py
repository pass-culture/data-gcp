import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_RAW_DATASET,
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
from common.utils import (
    get_airflow_schedule,
)

DAG_NAME = "metabase_governance"
GCE_INSTANCE = f"metabase-governance-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/metabase-governance"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": on_failure_vm_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import metabase tables from CloudSQL & archive old cards",
    schedule=get_airflow_schedule("00 08 * * *") if ENV_SHORT_NAME == "prod" else None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "taxonomy_source_dataset": Param(default=BIGQUERY_RAW_DATASET, type="string"),
        "taxonomy_source_table": Param(default="metabase_collection", type="string"),
        "taxonomy_destination_dataset": Param(
            default=BIGQUERY_RAW_DATASET, type="string"
        ),
        "taxonomy_destination_table": Param(
            default="collection_taxonomy", type="string"
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
        python_version="3.13",
        base_dir=BASE_PATH,
        retries=2,
        dag=dag,
    )

    archive_metabase_cards_op = SSHGCEOperator(
        task_id="archive_metabase_cards_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py archive ",
        do_xcom_push=True,
    )

    sync_permissions_op = SSHGCEOperator(
        task_id="sync_permissions_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py permissions ",
        do_xcom_push=True,
    )

    compute_metabase_dependencies_op = SSHGCEOperator(
        task_id="compute_metabase_dependencies_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py dependencies ",
        do_xcom_push=True,
    )

    compute_metabase_taxonomy_op = SSHGCEOperator(
        task_id="compute_metabase_taxonomy_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command=(
            "uv run python main.py taxonomy "
            "--dataset-name {{ params.taxonomy_source_dataset }} "
            "--table-name {{ params.taxonomy_source_table }} "
            "--destination-dataset {{ params.taxonomy_destination_dataset }} "
            "--destination-table {{ params.taxonomy_destination_table }}"
        ),
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        gce_instance_start
        >> fetch_install_code
        >> archive_metabase_cards_op
        >> sync_permissions_op
        >> compute_metabase_dependencies_op
        >> compute_metabase_taxonomy_op
        >> gce_instance_stop
    )
