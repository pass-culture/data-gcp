import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common import macros
from dependencies.qualtrics.export_qualtrics_data import export_tables
from common.config import (
    DAG_FOLDER,
    GCP_PROJECT,
    ENV_SHORT_NAME,
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    BIGQUERY_CLEAN_DATASET,
)
from common.alerts import task_fail_slack_alert
from common.operator import bigquery_job_task
from common.utils import depends_loop
from common.access_gcp_secrets import access_secret_data
import pandas as pd
from io import StringIO
import requests
from airflow.operators.python import PythonOperator

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    "export_qualtrics_data",
    default_args=default_dag_args,
    description="Export user data for Qualtrics usages",
    schedule_interval="00 06 01 * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

QUALTRICS_TOKEN = access_secret_data(GCP_PROJECT, f"qualtrics_token_{ENV_SHORT_NAME}")
QUALTRICS_DATA_CENTER = access_secret_data(
    GCP_PROJECT, f"qualtrics_data_center_{ENV_SHORT_NAME}"
)
QUALTRICS_BASE_URL = f"https://{QUALTRICS_DATA_CENTER}.qualtrics.com/automations-file-service/automations/"


def get_and_send(**kwargs):
    ds = datetime.datetime.strptime(kwargs["ds"], "%Y-%m-%d")
    table_name = kwargs["table_name"]
    dataset_id = kwargs["dataset_id"]
    automation_id = kwargs["automation_id"]
    current_month = ds.replace(day=1).strftime("%Y-%m-%d")
    sql = f"""
        WITH user_email AS (
            SELECT * FROM EXTERNAL_QUERY(
                        '{APPLICATIVE_EXTERNAL_CONNECTION_ID}',
                        ' SELECT CAST("id" AS varchar(255)) AS user_id, email FROM public.user '
                    )
        )
    SELECT 
        ue.email, 
        t.* except(calculation_month)
    FROM `{dataset_id}.{table_name}` t
    LEFT JOIN user_email ue on ue.user_id = t.user_id
    WHERE t.calculation_month = '{ current_month }'
    """
    df = pd.read_gbq(sql)
    export = df.to_csv(index=False)
    res = requests.post(
        f"{QUALTRICS_BASE_URL}{automation_id}/files",
        headers={"X-API-TOKEN": QUALTRICS_TOKEN},
        files={"file": ("data.csv", export)},
    )
    print(res.json())


start = DummyOperator(task_id="start", dag=dag)
table_jobs = {}
for table, job_params in export_tables.items():
    task = bigquery_job_task(dag, table, job_params)
    table_jobs[table] = {
        "operator": task,
        "depends": job_params.get("depends", []),
    }
    export_task = PythonOperator(
        task_id=f"export_to_qualtrics_{table}",
        python_callable=get_and_send,
        provide_context=True,
        op_kwargs={
            "table_name": table,
            "dataset_id": BIGQUERY_CLEAN_DATASET,
            "automation_id": job_params["qualtrics_automation_id"],
        },
        dag=dag,
    )
    export_task.set_upstream(task)

table_jobs = depends_loop(table_jobs, start)
