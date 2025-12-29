import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from common import macros
from common.access_gcp_secrets import access_secret_data
from common.callback import on_failure_vm_callback
from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_job_task
from common.utils import get_airflow_schedule

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_vm_callback,
}

dag = DAG(
    "export_qualtrics_data",
    default_args=default_dag_args,
    description="Export user data for Qualtrics usages",
    schedule_interval=get_airflow_schedule("00 06 25 * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DE.value],
)

QUALTRICS_TOKEN = access_secret_data(
    GCP_PROJECT_ID, f"qualtrics_token_{ENV_SHORT_NAME}"
)
QUALTRICS_DATA_CENTER = access_secret_data(
    GCP_PROJECT_ID, f"qualtrics_data_center_{ENV_SHORT_NAME}"
)
QUALTRICS_BASE_URL = f"https://{QUALTRICS_DATA_CENTER}.qualtrics.com/automations-file-service/automations/"

EXPORT_TABLES = {
    "qualtrics_exported_beneficiary_account": {
        "sql": "dependencies/qualtrics/sql/raw/qualtrics_exported_template.sql",
        "destination_dataset": BIGQUERY_RAW_DATASET,
        "destination_table": "qualtrics_exported_beneficiary_account${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "params": {
            "volume": 8000 if ENV_SHORT_NAME == "prod" else 10,
            "table_name": "qualtrics_beneficiary_account",
        },
        "qualtrics_automation_id": access_secret_data(
            GCP_PROJECT_ID, f"qualtrics_ir_jeunes_automation_id_{ENV_SHORT_NAME}"
        ),
        "include_email": True,
        "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
    },
    "qualtrics_exported_venue_account": {
        "sql": "dependencies/qualtrics/sql/raw/qualtrics_exported_template.sql",
        "destination_dataset": BIGQUERY_RAW_DATASET,
        "destination_table": "qualtrics_exported_venue_account${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "params": {
            "volume": 3500 if ENV_SHORT_NAME == "prod" else 10,
            "table_name": "qualtrics_venue_account",
        },
        "qualtrics_automation_id": access_secret_data(
            GCP_PROJECT_ID, f"qualtrics_ir_ac_automation_id_{ENV_SHORT_NAME}"
        ),
        "include_email": False,
        "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
    },
}


def get_and_send(**kwargs):
    ds = datetime.datetime.strptime(kwargs["ds"], "%Y-%m-%d")
    table_name = kwargs["table_name"]
    dataset_id = kwargs["dataset_id"]
    automation_id = kwargs["automation_id"]
    include_email = kwargs["include_email"]
    execution_date = ds.strftime("%Y-%m-%d")
    if include_email:
        sql = f"""
            WITH user_email AS (
                SELECT * FROM EXTERNAL_QUERY(
                            '{APPLICATIVE_EXTERNAL_CONNECTION_ID}',
                            ' SELECT CAST("id" AS varchar(255)) AS user_id, email FROM public.user '
                        )
            )
        SELECT
            ue.email,
            CURRENT_TIMESTAMP() as export_date,
            t.* except(calculation_month, export_date)
        FROM `{dataset_id}.{table_name}` t
        LEFT JOIN user_email ue on ue.user_id = t.user_id
        WHERE t.calculation_month = date_trunc(date("{execution_date}"), month)
        """
    else:
        sql = f"""
        SELECT
            CURRENT_TIMESTAMP() as export_date,
            t.* except(calculation_month, export_date)
        FROM `{dataset_id}.{table_name}` t
        WHERE t.calculation_month = date_trunc(date("{execution_date}"), month)
        """
    df = pd.read_gbq(sql)
    export = df.to_csv(index=False)
    res = requests.post(
        f"{QUALTRICS_BASE_URL}{automation_id}/files",
        headers={"X-API-TOKEN": QUALTRICS_TOKEN},
        files={"file": ("data.csv", export)},
    )
    print(res.json())
    return 1


start = EmptyOperator(task_id="start", dag=dag)
clean_table_jobs = {}
for table, job_params in EXPORT_TABLES.items():
    # export table to raw table to store exported data before uploading to qualtrics
    task = bigquery_job_task(dag, table, job_params)
    task.set_upstream(start)

    export_task = PythonOperator(
        task_id=f"export_to_qualtrics_{table}",
        python_callable=get_and_send,
        provide_context=True,
        op_kwargs={
            "table_name": table,
            "dataset_id": job_params["destination_dataset"],
            "automation_id": job_params["qualtrics_automation_id"],
            "include_email": job_params["include_email"],
        },
        dag=dag,
    )
    export_task.set_upstream(task)
