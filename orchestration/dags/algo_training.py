import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.gcp_compute_operator import (
    GceInstanceStartOperator,
    GceInstanceStopOperator,
)
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.access_gcp_secrets import access_secret_data
from dependencies.config import GCP_PROJECT_ID, GCE_ZONE, ENV_SHORT_NAME


GCE_INSTANCE = os.environ.get("GCE_TRAINING_INSTANCE", "algo-training-dev")
MLFLOW_BUCKET_NAME = os.environ.get("MLFLOW_BUCKET_NAME", "mlflow-bucket-ehp")
if ENV_SHORT_NAME != "prod":
    MLFLOW_URL = "https://mlflow-ehp.internal-passculture.app/"
else:
    MLFLOW_URL = "https://mlflow.internal-passculture.app/"

DATE = "{{ts_nodash}}"
STORAGE_PATH = (
    f"gs://{MLFLOW_BUCKET_NAME}/algo_training_{ENV_SHORT_NAME}/algo_training_{DATE}"
)

SLACK_CONN_ID = "slack"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT_ID, "slack-conn-password")

DEFAULT = f"""cd data-gcp/algo_training
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export STORAGE_PATH={STORAGE_PATH}
export ENV_SHORT_NAME={ENV_SHORT_NAME}"""

default_args = {
    "start_date": datetime(2021, 5, 20),
    "on_failure_callback": task_fail_slack_alert,
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

    if ENV_SHORT_NAME == "dev":
        branch = "PC-9207-debug-timeout"
    if ENV_SHORT_NAME == "stg":
        branch = "master"
    if ENV_SHORT_NAME == "prod":
        branch = "production"

    FETCH_CODE = f'"if cd data-gcp; then git checkout master && git pull && git checkout {branch}; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp && git checkout {branch}; fi"'

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

    PREPROCESS = f""" '{DEFAULT}
        python preprocess.py'
    """

    preprocess = BashOperator(
        task_id="preprocessing",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {PREPROCESS}
        """,
        dag=dag,
    )

    SPLIT_DATA = f""" '{DEFAULT}
        python split_data.py'
    """

    split_data = BashOperator(
        task_id="split_data",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {SPLIT_DATA}
        """,
        dag=dag,
    )

    TRAINING = f""" '{DEFAULT}
        python train.py'
    """

    training = BashOperator(
        task_id="training",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {TRAINING}
        """,
        dag=dag,
    )

    POSTPROCESSING = f""" '{DEFAULT}
        python postprocess.py'
    """

    postprocess = BashOperator(
        task_id="postprocess",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {POSTPROCESSING}
        """,
        dag=dag,
    )

    EVALUATION = f""" '{DEFAULT}
        python evaluate.py'
    """

    evaluate = BashOperator(
        task_id="evaluate",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {EVALUATION}
        """,
        dag=dag,
    )

    SLACK_BLOCKS = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":robot_face: Entrainement de l'algo fini !",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Il faut aller checker si les métriques sont bonnes puis déployer la nouvelle version! :rocket:",
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Voir les métriques :chart_with_upwards_trend:",
                        "emoji": True,
                    },
                    "url": MLFLOW_URL,
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Déploiement :rocket:",
                        "emoji": True,
                    },
                    "url": "https://github.com/pass-culture/data-gcp/tree/master/recommendation/model",
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"Environnement: {ENV_SHORT_NAME}"}
            ],
        },
    ]

    send_slack_notif = SlackWebhookOperator(
        task_id="send_slack_notif",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_CONN_PASSWORD,
        blocks=SLACK_BLOCKS,
        username="Algo trainer robot",
        icon_emoji=":robot_face:",
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
        >> data_collect
        >> preprocess
        >> split_data
        >> training
        >> postprocess
        >> evaluate
        >> send_slack_notif
        >> gce_instance_stop
        >> end
    )
