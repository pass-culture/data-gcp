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
MLFLOW_BUCKET_NAME = os.environ.get("MLFLOW_BUCKET_NAME", "mlflow-bucket-ehp")
if ENV_SHORT_NAME != "prod":
    MLFLOW_URL = "https://mlflow-ehp.internal-passculture.app/"
else:
    MLFLOW_URL = "https://mlflow.internal-passculture.app/"

DATE = "{{ts_nodash}}"
STORAGE_PATH = f"gs://{MLFLOW_BUCKET_NAME}/algo_training_v2_deep_reco_{ENV_SHORT_NAME}/algo_training_v2_deep_reco_{DATE}"
MODEL_NAME = "v2_mf_reco"
SLACK_CONN_ID = "slack"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT_ID, "slack-conn-password")

DEFAULT = f"""cd data-gcp/algo_training
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export STORAGE_PATH={STORAGE_PATH}
export ENV_SHORT_NAME={ENV_SHORT_NAME}
export GCP_PROJECT_ID={GCP_PROJECT_ID}
export MODEL_NAME={MODEL_NAME}"""


def branch_function(ti, **kwargs):
    evaluate_ending = ti.xcom_pull(task_ids="evaluate")
    if evaluate_ending == "Metrics OK":
        return "deploy_model"
    return "send_slack_notif_fail"


default_args = {
    "start_date": datetime(2021, 5, 20),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "algo_training_v2_mf_reco",
    default_args=default_args,
    description="Continuous algorithm training for v2 recommendation algorithm",
    schedule_interval="0 7 * * 1",  # Train every monday at 07:00
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
        branch = "PC-12275-build-automatic-training-matrix-factorization"
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
        python train_{MODEL_NAME}.py'
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
        xcom_push=True,
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

    gce_instance_stop = GceInstanceStopOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    DEPLOY_COMMAND = f"""
    export REGION=europe-west1
    export MODEL_NAME=deep_reco_{ENV_SHORT_NAME}
    export RECOMMENDATION_MODEL_DIR={{{{ ti.xcom_pull(task_ids='training') }}}}

    export VERSION_NAME=v_{{{{ ts_nodash }}}}

    gcloud ai-platform versions create $VERSION_NAME \
        --model=$MODEL_NAME \
        --origin=$RECOMMENDATION_MODEL_DIR \
        --runtime-version=2.5 \
        --framework=tensorflow \
        --python-version=3.7 \
        --region=$REGION \
        --machine-type=n1-standard-2

    gcloud ai-platform versions set-default $VERSION_NAME \
        --model=$MODEL_NAME \
        --region=$REGION
    """

    deploy_model = BashOperator(
        task_id="deploy_model",
        bash_command=DEPLOY_COMMAND,
    )

    SLACK_BLOCKS = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":robot_face: Nouvelle version de l'algo 'v2_deep_reco' déployée ! :rocket:",
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
                    "url": MLFLOW_URL
                    + "#/experiments/"
                    + "{{ ti.xcom_pull(task_ids='training').split('/')[4] }}"
                    + "/runs/"
                    + "{{ ti.xcom_pull(task_ids='training').split('/')[5] }}",
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

    send_slack_notif_success = SlackWebhookOperator(
        task_id="send_slack_notif_success",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_CONN_PASSWORD,
        blocks=SLACK_BLOCKS,
        username=f"Algo trainer robot - {ENV_SHORT_NAME}",
        icon_emoji=":robot_face:",
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
        >> gce_instance_stop
        >> deploy_model
        >> send_slack_notif_success
    )
