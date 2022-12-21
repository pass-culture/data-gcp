import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from common import macros
from common.access_gcp_secrets import access_secret_data
from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT_ID,
    GCE_ZONE,
    ENV_SHORT_NAME,
    BIGQUERY_SANDBOX_DATASET,
    DAG_FOLDER,
)
from jobs.ml.constants import IMPORT_TRAINING_SQL_PATH

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
TRAIN_DIR = "/home/airflow/train"

# Algo reco
MODEL_NAME = "v1"
AI_MODEL_NAME = f"tf_model_reco_{ENV_SHORT_NAME}"
END_POINT_NAME = f"vertex_ai_{ENV_SHORT_NAME}"
SERVING_CONTAINER = "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-5:latest"

# Algo offres similaires
API_DOCKER_IMAGE = f"eu.gcr.io/{GCP_PROJECT_ID}/similar-offers:{ENV_SHORT_NAME}-latest"
SIMILAR_OFFER_MODEL_NAME = f"similar_offers_{ENV_SHORT_NAME}"
SIMILAR_OFFER_END_POINT_NAME = f"vertex_ai_similar_offers_{ENV_SHORT_NAME}"

MIN_NODES = 1
MAX_NODES = 10 if ENV_SHORT_NAME == "prod" else 1
SLACK_CONN_ID = "slack_analytics"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT_ID, "slack-conn-password")

DEFAULT = f"""cd data-gcp/algo_training
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export STORAGE_PATH={STORAGE_PATH}
export ENV_SHORT_NAME={ENV_SHORT_NAME}
export GCP_PROJECT_ID={GCP_PROJECT_ID}
export MODEL_NAME={MODEL_NAME}
export TRAIN_DIR={TRAIN_DIR}
"""


def branch_function(ti, **kwargs):
    evaluate_ending = ti.xcom_pull(task_ids="evaluate")
    if evaluate_ending == "Metrics OK":
        return "deploy_model"
    return "send_slack_notif_fail"


default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "algo_training_r_and_d",
    default_args=default_args,
    description="Custom training job",
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    create_deduplicated_offers_table = BigQueryExecuteQueryOperator(
        task_id="deduplicate_offers",
        sql=(IMPORT_TRAINING_SQL_PATH / "tmp" / "deduplicate_offers.sql").as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_SANDBOX_DATASET}.deduplicated_enriched_offer_data",
        dag=dag,
    )

    create_dataset_table = BigQueryExecuteQueryOperator(
        task_id="import_to_sandbox_training_data_bookings",
        sql=(
            IMPORT_TRAINING_SQL_PATH / "tmp" / "training_data_deduplicated_bookings.sql"
        ).as_posix(),
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_SANDBOX_DATASET}.training_data_bookings",
        dag=dag,
    )

    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_start_task",
        dag=dag,
    )

    FETCH_CODE = r'"if cd data-gcp; then git checkout master && git pull && git checkout {{ params.branch }} && git pull; else git clone https://github.com/pass-culture/data-gcp.git && cd data-gcp && git checkout {{ params.branch }} && git pull; fi"'

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
        pip install -r requirements.txt --user'
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
        python data_collect.py --dataset {BIGQUERY_SANDBOX_DATASET}'
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
        do_xcom_push=True,
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

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_stop_task",
    )

    SLACK_BLOCKS = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":robot_face: Entraînement de l'algo terminé ! :rocket:",
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
        >> create_deduplicated_offers_table
        >> create_dataset_table
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> data_collect
        >> preprocess
        >> split_data
        >> training
        >> postprocess
        >> evaluate
        >> gce_instance_stop
        >> send_slack_notif_success
    )
