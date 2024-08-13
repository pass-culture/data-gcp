import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)

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
    "metabase_transition",
    default_args=default_dag_args,
    description="Switch metabase tables following dbt migration",
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
        "metabase_card_type": Param(
            default="native",
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
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

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

    switch_metabase_cards_op = SSHGCEOperator(
        task_id="switch_metabase_cards_op",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="""
        python main.py \
        --metabase-card-type {{ params.metabase_card_type }} \
        --legacy-table-name {{ params.legacy_table_name }} \
        --new-table-name {{ params.new_table_name }} \
        --legacy-schema-name {{ params.legacy_schema_name }} \
        --new-schema-name {{ params.new_schema_name }}
        """,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> switch_metabase_cards_op
        >> gce_instance_stop
    )
