import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from common.utils import depends_loop
from dependencies.metabase.import_metabase import (
    import_tables,
    from_external,
    analytics_tables,
)
from common.config import METABASE_EXTERNAL_CONNECTION_ID
from common.config import GCP_PROJECT, DAG_FOLDER
from common.alerts import task_fail_slack_alert


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_metabase",
    default_args=default_dag_args,
    description="Import metabase tables from CloudSQL",
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

import_tables_to_raw_tasks = []
for name, params in import_tables.items():

    task = BigQueryExecuteQueryOperator(
        task_id=f"import_metabase_{name}_to_raw",
        sql=from_external(
            conn_id=METABASE_EXTERNAL_CONNECTION_ID,
            params=params,
        ),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=params["destination_dataset_table"],
        dag=dag,
    )
    import_tables_to_raw_tasks.append(task)

end_raw = DummyOperator(task_id="end_raw", dag=dag)


# import_tables_to_analytics_tasks = []
analytics_table_jobs = {}
for name, params in analytics_tables.items():

    task = BigQueryExecuteQueryOperator(
        task_id=f"{name}",
        sql=params["sql"],
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=params["destination_dataset_table"],
        dag=dag,
    )

    analytics_table_jobs[name] = {
        "operator": task,
        "depends": params.get("depends", []),
    }

    # import_tables_to_analytics_tasks.append(task)

analytics_table_tasks = depends_loop(analytics_table_jobs, end_raw)
end = DummyOperator(task_id="end", dag=dag)


(start >> import_tables_to_raw_tasks >> end_raw >> analytics_table_tasks >> end)
