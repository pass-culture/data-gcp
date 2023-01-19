import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

from common import macros
from common.utils import (
    getting_service_account_token,
    depends_loop,
    get_airflow_schedule,
)

from common.config import GCP_PROJECT_ID, DAG_FOLDER, ENV_SHORT_NAME
from common.config import GCP_PROJECT_ID, DAG_FOLDER
from common.alerts import task_fail_slack_alert
from dependencies.cold_data.import_cold_data import analytics_tables

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "import_cold_data",
    default_args=default_dag_args,
    description="Import cold data from GCS to BQ",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

service_account_token = PythonOperator(
    task_id="getting_cold_data_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": f"cold_data_{ENV_SHORT_NAME}"},
    dag=dag,
)

import_cold_data_op = SimpleHttpOperator(
    task_id=f"import_cold_data",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=f"cold_data_{ENV_SHORT_NAME}",
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_cold_data_service_account_token', key='return_value')}}",
    },
    log_response=True,
    dag=dag,
)

end_raw = DummyOperator(task_id="end_raw", dag=dag)

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
        "dag_depends": params.get("dag_depends", []),
    }

analytics_table_tasks = depends_loop(analytics_table_jobs, end_raw, dag=dag)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> service_account_token
    >> import_cold_data_op
    >> end_raw
    >> analytics_table_tasks
    >> end
)
