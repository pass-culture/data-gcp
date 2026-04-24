import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
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

DAG_NAME = "metabase_transition"
GCE_INSTANCE = f"metabase-transition-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/metabase"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Migrate Metabase cards after BigQuery table/column renames",
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "database_name": Param(
            default="Pass Culture BigQuery",
            type="string",
        ),
        "legacy_table_name": Param(
            default="",
            type="string",
        ),
        "new_table_name": Param(
            default="",
            type="string",
        ),
        "legacy_schema_name": Param(
            default=BIGQUERY_ANALYTICS_DATASET,
            type="string",
        ),
        "new_schema_name": Param(
            default=BIGQUERY_ANALYTICS_DATASET,
            type="string",
        ),
        "dry_run": Param(
            default=False,
            type="boolean",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

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
        dag=dag,
        retries=2,
    )

    switch_metabase_cards_op = SSHGCEOperator(
        task_id="switch_metabase_cards_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="""
        uv run python main.py migrate \
        --database-name "{{ params.database_name }}" \
        --legacy-table-name "{{ params.legacy_table_name }}" \
        --new-table-name "{{ params.new_table_name }}" \
        --legacy-schema-name "{{ params.legacy_schema_name }}" \
        --new-schema-name "{{ params.new_schema_name }}" \
        {% if params.dry_run %}--dry-run{% endif %}
        """,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> switch_metabase_cards_op
        >> gce_instance_stop
    )
