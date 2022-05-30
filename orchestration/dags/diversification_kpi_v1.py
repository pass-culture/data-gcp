import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from common.alerts import task_fail_slack_alert
from dependencies.access_gcp_secrets import access_secret_data
from dependencies.config import GCP_PROJECT_ID, GCE_ZONE, ENV_SHORT_NAME


GCE_INSTANCE = os.environ.get("GCE_DIVERSIFICATION_INSTANCE", "algo-training-dev")

DATE = "{{ts_nodash}}"

SLACK_CONN_ID = "slack"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT_ID, "slack-conn-password")
TABLE_NAME = "diversification_booking"

DEFAULT = f"""cd data-gcp/diversification_kpi
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export ENV_SHORT_NAME={ENV_SHORT_NAME}
export GCP_PROJECT={GCP_PROJECT_ID}
export TABLE_NAME={TABLE_NAME}
"""


default_args = {
    "start_date": datetime(2022, 4, 5),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "diversification_kpi",
    default_args=default_args,
    description="Measure the diversification",
    schedule_interval="0 4 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
) as dag:

    start = DummyOperator(task_id="start")

    delete_old_table = BigQueryDeleteTableOperator(
        task_id="delete_old_table",
        deletion_dataset_table=f"{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.{TABLE_NAME}",
        ignore_if_missing=True,
        dag=dag,
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        project_id=GCP_PROJECT_ID,
        dataset_id=f"analytics_{ENV_SHORT_NAME}",
        table_id=TABLE_NAME,
        schema_fields=[
            {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "offer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "booking_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "booking_creation_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "subcategory", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "venue", "type": "STRING", "mode": "NULLABLE"},
            {"name": "venue_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "user_region_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "user_department_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "user_activity", "type": "STRING", "mode": "NULLABLE"},
            {"name": "user_civility", "type": "STRING", "mode": "NULLABLE"},
            {"name": "booking_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {
                "name": "user_deposit_creation_date",
                "type": "TIMESTAMP",
                "mode": "NULLABLE",
            },
            {"name": "format", "type": "STRING", "mode": "NULLABLE"},
            {"name": "macro_rayon", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category_diversification", "type": "FLOAT", "mode": "NULLABLE"},
            {
                "name": "subcategory_diversification",
                "type": "FLOAT",
                "mode": "NULLABLE",
            },
            {"name": "format_diversification", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "venue_diversification", "type": "FLOAT", "mode": "NULLABLE"},
            {
                "name": "macro_rayon_diversification",
                "type": "FLOAT",
                "mode": "NULLABLE",
            },
            {"name": "qpi_diversification", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "delta_diversification", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_start_task",
    )

    if ENV_SHORT_NAME == "dev":
        branch = "PC-14596-fix-bug-and-improve-diversification"
    if ENV_SHORT_NAME == "stg":
        branch = "master"
    if ENV_SHORT_NAME == "prod":
        branch = "production"

    FETCH_CODE = f'"if cd data-gcp; then git checkout master && git pull && git checkout {branch} && git pull; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp && git checkout {branch} && git pull; fi"'

    fetch_code = BashOperator(
        task_id="fetch_code",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {FETCH_CODE}
        """,
        dag=dag,
    )

    INSTALL_DEPENDENCIES = f""" '{DEFAULT}
        pip install -r requirements.txt'
    """

    install_dependencies = BashOperator(
        task_id="install_dependencies",
        bash_command=f"""
            gcloud compute ssh {GCE_INSTANCE} \
            --zone {GCE_ZONE} \
            --project {GCP_PROJECT_ID} \
            --command {INSTALL_DEPENDENCIES}
            """,
        dag=dag,
    )

    DIVERSIFICATION_MEASURE = f""" '{DEFAULT}
        python diversification_data.py'
    """

    data_collect = BashOperator(
        task_id="data_collect",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {DIVERSIFICATION_MEASURE}
        """,
        dag=dag,
    )

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    (start >> delete_old_table >> create_table >> data_collect)

    (start >> gce_instance_start >> fetch_code >> install_dependencies >> data_collect)
    data_collect >> gce_instance_stop
