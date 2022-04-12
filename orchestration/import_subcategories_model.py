import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.gcp_compute_operator import (
    GceInstanceStartOperator,
    GceInstanceStopOperator,
)
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.access_gcp_secrets import access_secret_data
from dependencies.config import GCP_PROJECT_ID, GCE_ZONE, ENV_SHORT_NAME


GCE_INSTANCE = os.environ.get("GCE_TRAINING_INSTANCE", "algo-training-dev")

STORAGE_PATH = f"gs://{MLFLOW_BUCKET_NAME}/algo_training_v2_deep_reco_{ENV_SHORT_NAME}/algo_training_v2_deep_reco_{DATE}"
SLACK_CONN_ID = "slack"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT_ID, "slack-conn-password")

DEFAULT = f"""cd data-gcp/analytics/scripts/import_subcategories_model
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export ENV_SHORT_NAME={ENV_SHORT_NAME}
export GCP_PROJECT_ID={GCP_PROJECT_ID}
"""

DEFAULT_NATIVE = f"""cd data-gcp/analytics/scripts/import_subcategories_model
export STORAGE_PATH={STORAGE_PATH}
export ENV_SHORT_NAME={ENV_SHORT_NAME}
export GCP_PROJECT_ID={GCP_PROJECT_ID}"""

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "import_subcategories_model_from_app",
    default_args=default_args,
    description="Continuous update subcategories model to BQ",
    schedule_interval="0 0 * * 1",  # import every monday at 00:00
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
) as dag:

    start = DummyOperator(task_id="start")

    gce_instance_start = GceInstanceStartOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_start_task",
    )

    if ENV_SHORT_NAME == "dev":
        branch = "PC-14100-import-subcategories-model"
    if ENV_SHORT_NAME == "stg":
        branch = "master"
    if ENV_SHORT_NAME == "prod":
        branch = "production"

    FETCH_CODE = f'"if cd data-gcp; then git checkout master && git pull && git checkout {branch} && git pull; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp && git checkout {branch} && git pull; fi"'

    FETCH_CODE_NATIVE = f'"if cd pass-culture-main; then git checkout master && git pull ; else git clone git@github.com:pass-culture/pass-culture-main.git && git pull; fi"'

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
    # HERE WE FETCH THE SECOND REPOSITORY
    fetch_code_native = BashOperator(
        task_id="fetch_code_native",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {FETCH_CODE_NATIVE}
        """,
        dag=dag,
    )

    SETUP_PYTHON_SCRIPT = f""" '{DEFAULT_NATIVE}'
        cp export_subcategories_from_native_definition.py pass-culture-main/export_subcategories_from_native_definition.py
        cd pass-culture-main/
        export SCRIPTPATH=pass-culture-main/api/src
        export PYTHONPATH="$pwd$SCRIPTPATH"+$PYTHONPATH"""

    EXPORT_SUBCAT = f""" '{DEFAULT_NATIVE}
        python3.9 export_subcategories_from_native_definition.py'
    """

    export_subcategories = BashOperator(
        task_id="export_subcategories",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {EXPORT_SUBCAT}
        """,
        dag=dag,
    )

    gce_instance_stop = GceInstanceStopOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> fetch_code_native
        >> export_subcategories
    )
