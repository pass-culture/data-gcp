import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from common.alerts import task_fail_slack_alert
from common.config import GCP_PROJECT_ID, GCE_ZONE, ENV_SHORT_NAME


GCE_INSTANCE = os.environ.get(
    "GCE_TRAINING_INSTANCE", f"algo-training-{ENV_SHORT_NAME}"
)
BASE_PATH = "data-gcp"
SCRIPT_PATH = "analytics/scripts/import_api_referentials"

default_args = {
    "start_date": datetime(2022, 4, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "import_api_referentials",
    default_args=default_args,
    description="Continuous update of api model to BQ",
    schedule_interval="0 0 * * 1",  # import every monday at 00:00
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
) as dag:

    start = DummyOperator(task_id="start")

    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_start_task",
    )

    if ENV_SHORT_NAME == "dev":
        branch = "PC-14911-table-de-correspondance-music-type-show-type"
    if ENV_SHORT_NAME == "stg":
        branch = "master"
    if ENV_SHORT_NAME == "prod":
        branch = "production"

    FETCH_CODE = f"""
        
        if cd data-gcp; then git checkout master && git pull && git checkout {branch} && git pull; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp && git checkout {branch} && git pull; fi
        cd {SCRIPT_PATH}
        if cd pass-culture-main; then git checkout master && git reset --hard origin/master; else git clone git@github.com:pass-culture/pass-culture-main.git && cd pass-culture-main && git checkout master && git pull; fi
        cp -r api/src/pcapi ..
        """

    fetch_code = BashOperator(
        task_id="fetch_code",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command '{FETCH_CODE}'
        """,
        dag=dag,
    )

    INSTALL_DEPENDENCIES = f"""
        cd {BASE_PATH}/{SCRIPT_PATH}
        export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
        pip install -r requirements.txt
    """

    install_dependencies = BashOperator(
        task_id="install_dependencies",
        bash_command=f"""
            gcloud compute ssh {GCE_INSTANCE} \
            --zone {GCE_ZONE} \
            --project {GCP_PROJECT_ID} \
            --command '{INSTALL_DEPENDENCIES}'
            """,
        dag=dag,
    )
    tasks = []
    for job_type in ["subcategories", "types"]:

        EXPORT_SUBCAT = f""" 
            cd {BASE_PATH}/{SCRIPT_PATH}
            python3.9 main.py --job_type={job_type} --gcp_project_id={GCP_PROJECT_ID} --env_short_name={ENV_SHORT_NAME}
        """

        import_job = BashOperator(
            task_id=f"import_{job_type}",
            bash_command=f"""
            gcloud compute ssh {GCE_INSTANCE} \
            --zone {GCE_ZONE} \
            --project {GCP_PROJECT_ID} \
            --command '{EXPORT_SUBCAT}'
            """,
            dag=dag,
        )
        tasks.append(import_job)

    gce_instance_stop = ComputeEngineStopInstanceOperator(
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
        >> tasks
        >> gce_instance_stop
    )
