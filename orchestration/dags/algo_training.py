import os
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcp_compute_operator import (
    GceInstanceStartOperator,
    GceInstanceStopOperator,
)
from dependencies.slack_alert import task_fail_slack_alert

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "passculture-data-ehp")
GCE_ZONE = os.environ.get("GCE_ZONE", "europe-west1-b")
GCE_INSTANCE = os.environ.get("GCE_INSTANCE", "algo-training-dev")


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

    COMMAND = """
    'cat /dev/zero | ssh-keygen -t ed25519 -C composer-dev@passculture-data-ehp.iam.gserviceaccount.com -f "/home/airflow/.ssh/id_ed25519" -N ""
    if cd data-gcp; then git pull; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp; fi
    export VAR_ENV="plop"'
    """

    fetch_code = BashOperator(
        task_id="fetch_code",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {COMMAND}
        """,
        dag=dag,
    )

    gce_instance_stop = GceInstanceStopOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    start >> gce_instance_start >> fetch_code >> gce_instance_stop >> end
