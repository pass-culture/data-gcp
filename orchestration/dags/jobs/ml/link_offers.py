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
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
)

GCE_INSTANCE = os.environ.get("GCE_TRAINING_INSTANCE", "algo-training-dev-1")

DATE = "{{ts_nodash}}"
STORAGE_PATH = f"gs://{MLFLOW_BUCKET_NAME}/link_offers_{ENV_SHORT_NAME}/linkage_{DATE}"
# Algo reco
DEFAULT = f"""cd data-gcp/record_linkage
export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH
export STORAGE_PATH={STORAGE_PATH}
export ENV_SHORT_NAME={ENV_SHORT_NAME}
export GCP_PROJECT_ID={GCP_PROJECT_ID}
"""


def branch_function(ti, **kwargs):
    evaluate_ending = ti.xcom_pull(task_ids="evaluate")
    if evaluate_ending == "Metrics OK":
        return "deploy_model"
    return "send_slack_notif_fail"


default_args = {
    "start_date": datetime(2022, 12, 13),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "link_offers",
    default_args=default_args,
    description="Link offers via recordLinkage",
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

    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        task_id="gce_start_task",
        dag=dag,
    )

    FETCH_CODE = r'"if cd data-gcp; then git fetch origin {{ params.branch }} && git checkout {{ params.branch }}; else git clone git@github.com:pass-culture/data-gcp.git && cd data-gcp && git checkout {{ params.branch }}; fi"'

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
        python data_collect.py --gcp_project {GCP_PROJECT_ID} --env_short_name {ENV_SHORT_NAME} --storage_path {STORAGE_PATH}'
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
        python preprocess.py --storage_path {STORAGE_PATH}'
    """

    preprocess = BashOperator(
        task_id="preprocess",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {PREPROCESS}
        """,
        dag=dag,
    )

    RECORD_LINKAGE = f""" '{DEFAULT}
        python main.py --storage_path {STORAGE_PATH}'
    """

    record_linkage = BashOperator(
        task_id="record_linkage",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {RECORD_LINKAGE}
        """,
        dag=dag,
    )

    POSTPROCESS = f""" '{DEFAULT}
        python postprocess.py --gcp_project {GCP_PROJECT_ID} --env_short_name {ENV_SHORT_NAME} --storage_path {STORAGE_PATH}'
    """

    postprocess = BashOperator(
        task_id="postprocess",
        bash_command=f"""
        gcloud compute ssh {GCE_INSTANCE} \
        --zone {GCE_ZONE} \
        --project {GCP_PROJECT_ID} \
        --command {POSTPROCESS}
        """,
        dag=dag,
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> data_collect
        >> preprocess
        >> record_linkage
        >> postprocess
    )
