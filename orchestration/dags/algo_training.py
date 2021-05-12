import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcp_compute_operator import (
    GceInstanceStartOperator,
    GceInstanceStopOperator,
)
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.config import GCP_PROJECT_ID, GCE_ZONE


GCE_INSTANCE = os.environ.get("GCE_TRAINING_INSTANCE", "algo-training-dev")
MLFLOW_BUCKET_NAME = os.environ.get("MLFLOW_BUCKET_NAME", "mlflow-bucket-ehp")

DATE = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
STORAGE_PATH = f"gs://{MLFLOW_BUCKET_NAME}/algo_training/algo_training_{DATE}"

DEFAULT = f""" cd data-gcp/algo_training
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export STORAGE_PATH={STORAGE_PATH}
"""

default_args = {
    "start_date": datetime(2021, 5, 5),
    # "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "algo_training_v1",
    default_args=default_args,
    description="Continuous algorithm training",
    schedule_interval="@once",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    gce_instance_start = GceInstanceStartOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_start_task",
    )

    FETCH_CODE = '"if cd data-gcp; then git pull; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp; fi"'

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

    DATA_COLLECT = f""" '{DEFAULT}
python data_collect.py'
"""

    data_collect = BashOperator(
        task_id="data_collect",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {DATA_COLLECT}
        """,
        dag=dag,
    )

    FEATURE_ENG = f""" '{DEFAULT}
python feature_engineering.py'
"""

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {FEATURE_ENG}
        """,
        dag=dag,
    )

    gce_instance_stop = GceInstanceStopOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    start >> gce_instance_start
    gce_instance_start >> fetch_code >> data_collect >> feature_engineering >> gce_instance_stop
    gce_instance_stop >> end
