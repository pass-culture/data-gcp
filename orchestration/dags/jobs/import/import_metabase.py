import datetime

from common import macros
from common.callback import on_failure_base_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    METABASE_EXTERNAL_CONNECTION_ID,
)
from common.utils import (
    get_airflow_schedule,
)
from dependencies.metabase.import_metabase import (
    from_external,
    import_tables,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": on_failure_base_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    "import_metabase",
    default_args=default_dag_args,
    description="Import metabase tables from CloudSQL",
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
    tags=[DAG_TAGS.DE.value],
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    import_tables_to_raw_tasks = []
    for name, params in import_tables.items():
        task = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
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

    (start >> import_tables_to_raw_tasks >> end_raw)
