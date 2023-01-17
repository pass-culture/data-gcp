import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common import macros
from common.utils import depends_loop, getting_service_account_token
from dependencies.metabase.import_metabase import (
    import_tables,
    from_external,
    analytics_tables,
)
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    METABASE_EXTERNAL_CONNECTION_ID,
    ENV_SHORT_NAME,
)
from common.config import GCP_PROJECT_ID, DAG_FOLDER
from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "metabase_governance",
    default_args=default_dag_args,
    description="Import metabase tables from CloudSQL & archive old cards",
    schedule_interval="00 01 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

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
                "writeDisposition": params.get("write_disposition", "WRITE_TRUNCATE"),
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

analytics_table_tasks = depends_loop(analytics_table_jobs, end_raw, dag=dag)

service_account_token = PythonOperator(
    task_id="getting_metabase_archiving_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": f"metabase_archiving_{ENV_SHORT_NAME}"},
    dag=dag,
)

archive_metabase_cards_op = SimpleHttpOperator(
    task_id=f"archive_metabase_cards",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"metabase_archiving_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_metabase_archiving_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)


(
    start
    >> import_tables_to_raw_tasks
    >> end_raw
    >> analytics_table_tasks
    >> service_account_token
    >> archive_metabase_cards_op
    >> end
)
