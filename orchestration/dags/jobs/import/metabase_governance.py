import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    METABASE_EXTERNAL_CONNECTION_ID,
)
from common.operators.biquery import bigquery_job_task
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import (
    depends_loop,
    get_airflow_schedule,
)
from dependencies.metabase.import_metabase import (
    analytics_tables,
    from_external,
    import_tables,
)

GCE_INSTANCE = f"metabase-governance-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/metabase-archiving"
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
    "metabase_governance",
    default_args=default_dag_args,
    description="Import metabase tables from CloudSQL & archive old cards",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
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
    start = DummyOperator(task_id="start", dag=dag)

    import_tables_to_raw_tasks = []
    for name, params in import_tables.items():
        task = BigQueryInsertJobOperator(
            task_id=f"import_metabase_{name}_to_raw",
            configuration={
                "query": {
                    "query": from_external(
                        conn_id=METABASE_EXTERNAL_CONNECTION_ID, params=params
                    ),
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": params["destination_dataset"],
                        "tableId": params["destination_table"],
                    },
                    "writeDisposition": params.get(
                        "write_disposition", "WRITE_TRUNCATE"
                    ),
                }
            },
            params=dict(params.get("params", {})),
            dag=dag,
        )
        import_tables_to_raw_tasks.append(task)

    end_raw = DummyOperator(task_id="end_raw", dag=dag)

    analytics_table_jobs = {}
    for name, params in analytics_tables.items():
        task = bigquery_job_task(dag, name, params)
        analytics_table_jobs[name] = {
            "operator": task,
            "depends": params.get("depends", []),
            "dag_depends": params.get("dag_depends", []),
        }
    end = DummyOperator(task_id="end", dag=dag)

    analytics_table_tasks = depends_loop(
        analytics_tables,
        analytics_table_jobs,
        end_raw,
        dag=dag,
        default_end_operator=end,
    )

    if ENV_SHORT_NAME == "prod":
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

        archive_metabase_cards_op = SSHGCEOperator(
            task_id="archive_metabase_cards_op",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_PATH,
            environment=dag_config,
            command="python main.py ",
            do_xcom_push=True,
        )

        compute_metabase_dependencies_op = SSHGCEOperator(
            task_id="compute_metabase_dependencies_op",
            instance_name=GCE_INSTANCE,
            base_dir=BASE_PATH,
            environment=dag_config,
            command="python dependencies.py ",
            do_xcom_push=True,
        )

        gce_instance_stop = StopGCEOperator(
            task_id="gce_stop_task", instance_name=GCE_INSTANCE
        )

        (
            analytics_table_tasks
            >> gce_instance_start
            >> fetch_code
            >> install_dependencies
            >> archive_metabase_cards_op
            >> compute_metabase_dependencies_op
            >> gce_instance_stop
        )

    (start >> import_tables_to_raw_tasks >> end_raw >> analytics_table_tasks)
