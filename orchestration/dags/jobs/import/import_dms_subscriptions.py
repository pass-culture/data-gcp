from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from common.alerts import task_fail_slack_alert
from common.operators.biquery import bigquery_job_task


from common.config import (
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    DAG_FOLDER,
)

from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)

from dependencies.dms_subscriptions.import_dms_subscriptions import CLEAN_TABLES
from common import macros
from common.utils import get_airflow_schedule


DMS_FUNCTION_NAME = "dms_" + ENV_SHORT_NAME
GCE_INSTANCE = f"import-dms-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/dms"

default_args = {
    "start_date": datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "import_dms_subscriptions",
    default_args=default_args,
    description="Import DMS subscriptions",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "updated_since_jeunes": Param(
            default=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
            type="string",
        ),
        "updated_since_pro": Param(
            default=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
            type="string",
        ),
    },
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
) as dag:

    start = DummyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        retries=2,
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.8",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

    dms_to_gcs_pro = SSHGCEOperator(
        task_id=f"dms_to_gcs_pro",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python main.py pro {{ params.updated_since_pro }} "
        + f"{GCP_PROJECT_ID} {ENV_SHORT_NAME}",
        do_xcom_push=True,
    )

    dms_to_gcs_jeunes = SSHGCEOperator(
        task_id=f"dms_to_gcs_jeunes",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python main.py jeunes {{ params.updated_since_jeunes }} "
        + f"{GCP_PROJECT_ID} {ENV_SHORT_NAME}",
        do_xcom_push=True,
    )

    parse_api_result_jeunes = SSHGCEOperator(
        task_id="parse_api_result_jeunes",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python parse_dms_subscriptions_to_tabular.py --target jeunes --updated-since {{ params.updated_since_jeunes }} "
        + f"--bucket-name {DATA_GCS_BUCKET_NAME} --project-id {GCP_PROJECT_ID}",
        do_xcom_push=True,
    )

    parse_api_result_pro = SSHGCEOperator(
        task_id="parse_api_result_pro",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="python parse_dms_subscriptions_to_tabular.py --target pro --updated-since {{ params.updated_since_pro }} "
        + f"--bucket-name {DATA_GCS_BUCKET_NAME} --project-id {GCP_PROJECT_ID}",
        do_xcom_push=True,
    )

    import_dms_jeunes_to_bq = GCSToBigQueryOperator(
        task_id="import_dms_jeunes_to_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "dms_export/dms_jeunes_{{ params.updated_since_jeunes }}.parquet"
        ],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.dms_jeunes",
        schema_fields=[
            {"name": "procedure_id", "type": "STRING"},
            {"name": "application_id", "type": "STRING"},
            {"name": "application_number", "type": "STRING"},
            {"name": "application_archived", "type": "STRING"},
            {"name": "application_status", "type": "STRING"},
            {"name": "last_update_at", "type": "TIMESTAMP"},
            {"name": "application_submitted_at", "type": "TIMESTAMP"},
            {"name": "passed_in_instruction_at", "type": "TIMESTAMP"},
            {"name": "processed_at", "type": "TIMESTAMP"},
            {"name": "application_motivation", "type": "STRING"},
            {"name": "instructors", "type": "STRING"},
            {"name": "applicant_department", "type": "STRING"},
            {"name": "applicant_postal_code", "type": "STRING"},
            {"name": "update_date", "type": "TIMESTAMP"},
        ],
        write_disposition="WRITE_APPEND",
    )

    import_dms_pro_to_bq = GCSToBigQueryOperator(
        task_id="import_dms_pro_to_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=["dms_export/dms_pro_{{ params.updated_since_pro }}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.dms_pro",
        schema_fields=[
            {"name": "procedure_id", "type": "STRING"},
            {"name": "application_id", "type": "STRING"},
            {"name": "application_number", "type": "STRING"},
            {"name": "application_archived", "type": "STRING"},
            {"name": "application_status", "type": "STRING"},
            {"name": "last_update_at", "type": "TIMESTAMP"},
            {"name": "application_submitted_at", "type": "TIMESTAMP"},
            {"name": "passed_in_instruction_at", "type": "TIMESTAMP"},
            {"name": "processed_at", "type": "TIMESTAMP"},
            {"name": "application_motivation", "type": "STRING"},
            {"name": "instructors", "type": "STRING"},
            {"name": "demandeur_siret", "type": "STRING"},
            {"name": "demandeur_naf", "type": "STRING"},
            {"name": "demandeur_libelleNaf", "type": "STRING"},
            {"name": "demandeur_entreprise_siren", "type": "STRING"},
            {"name": "demandeur_entreprise_formeJuridique", "type": "STRING"},
            {"name": "demandeur_entreprise_formeJuridiqueCode", "type": "STRING"},
            {"name": "demandeur_entreprise_codeEffectifEntreprise", "type": "STRING"},
            {"name": "demandeur_entreprise_raisonSociale", "type": "STRING"},
            {"name": "demandeur_entreprise_siretSiegeSocial", "type": "STRING"},
            {"name": "numero_identifiant_lieu", "type": "STRING"},
            {"name": "statut", "type": "STRING"},
            {"name": "typologie", "type": "STRING"},
            {"name": "academie_historique_intervention", "type": "STRING"},
            {"name": "academie_groupe_instructeur", "type": "STRING"},
            {"name": "domaines", "type": "STRING"},
            {"name": "erreur_traitement_pass_culture", "type": "STRING"},
            {"name": "update_date", "type": "INT64"},
        ],
        write_disposition="WRITE_APPEND",
    )

    cleaning_tasks = []
    for table, params in CLEAN_TABLES.items():
        task = bigquery_job_task(table=table, dag=dag, job_params=params)
        cleaning_tasks.append(task)

    end = DummyOperator(task_id="end")

    gce_instance_stop = StopGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task", dag=dag
    )

(
    start
    >> gce_instance_start
    >> fetch_code
    >> install_dependencies
    >> [dms_to_gcs_pro, dms_to_gcs_jeunes]
)
(
    dms_to_gcs_jeunes
    >> parse_api_result_jeunes
    >> import_dms_jeunes_to_bq
    >> cleaning_tasks[0]
    >> end
)
(
    dms_to_gcs_pro
    >> parse_api_result_pro
    >> import_dms_pro_to_bq
    >> cleaning_tasks[1]
    >> end
)
(end >> gce_instance_stop)
