import time
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_job_task
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule
from dependencies.dms_subscriptions.import_dms_subscriptions import CLEAN_TABLES

DMS_FUNCTION_NAME = "dms_" + ENV_SHORT_NAME
GCE_INSTANCE = f"import-dms-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/dms"
DAG_NAME = "import_dms_subscriptions"

dag_config = {
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_args = {
    "start_date": datetime(2020, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Import DMS subscriptions",
    schedule_interval=get_airflow_schedule("0 2 * * *"),
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    start = EmptyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        instance_type="n1-standard-4",
        task_id="gce_start_task",
        retries=2,
        labels={"job_type": "long_task", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.8",
        base_dir=BASE_PATH,
        dag=dag,
        retries=2,
    )

    dms_to_gcs_pro = SSHGCEOperator(
        task_id="dms_to_gcs_pro",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py pro {{ params.updated_since_pro }} ",
        do_xcom_push=True,
    )

    sleep_op = PythonOperator(
        dag=dag,
        task_id="sleep_task",
        python_callable=lambda: time.sleep(60),  # wait 1 minute
    )

    dms_to_gcs_jeunes = SSHGCEOperator(
        task_id="dms_to_gcs_jeunes",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py jeunes {{ params.updated_since_jeunes }} ",
        do_xcom_push=True,
    )

    parse_api_result_jeunes = SSHGCEOperator(
        task_id="parse_api_result_jeunes",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python parse_dms_subscriptions_to_tabular.py --target jeunes --updated-since {{ params.updated_since_jeunes }} "
        + f"--bucket-name {DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME} ",
        do_xcom_push=True,
    )

    parse_api_result_pro = SSHGCEOperator(
        task_id="parse_api_result_pro",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python parse_dms_subscriptions_to_tabular.py --target pro --updated-since {{ params.updated_since_pro }} "
        + f"--bucket-name {DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME} ",
        do_xcom_push=True,
    )

    import_dms_jeunes_to_bq = GCSToBigQueryOperator(
        project_id=GCP_PROJECT_ID,
        task_id="import_dms_jeunes_to_bq",
        bucket=DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME,
        source_objects=[
            "dms_export/dms_jeunes_{{ params.updated_since_jeunes }}.parquet"
        ],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.raw_dms_jeunes",
        schema_fields=[
            {"name": "procedure_id", "type": "STRING"},
            {"name": "application_id", "type": "STRING"},
            {"name": "application_number", "type": "STRING"},
            {"name": "application_archived", "type": "STRING"},
            {"name": "application_status", "type": "STRING"},
            {"name": "last_update_at", "type": "INT64"},
            {"name": "application_submitted_at", "type": "INT64"},
            {"name": "passed_in_instruction_at", "type": "INT64"},
            {"name": "processed_at", "type": "INT64"},
            {"name": "instructors", "type": "STRING"},
            {"name": "applicant_department", "type": "STRING"},
            {"name": "applicant_postal_code", "type": "STRING"},
            {"name": "update_date", "type": "INT64"},
        ],
        write_disposition="WRITE_APPEND",
    )

    import_dms_pro_to_bq = GCSToBigQueryOperator(
        project_id=GCP_PROJECT_ID,
        task_id="import_dms_pro_to_bq",
        bucket=DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME,
        source_objects=["dms_export/dms_pro_{{ params.updated_since_pro }}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.raw_dms_pro",
        schema_fields=[
            {"name": "procedure_id", "type": "STRING"},
            {"name": "application_id", "type": "STRING"},
            {"name": "application_number", "type": "STRING"},
            {"name": "application_archived", "type": "STRING"},
            {"name": "application_status", "type": "STRING"},
            {"name": "last_update_at", "type": "INT64"},
            {"name": "application_submitted_at", "type": "INT64"},
            {"name": "passed_in_instruction_at", "type": "INT64"},
            {"name": "processed_at", "type": "INT64"},
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

    end = EmptyOperator(task_id="end")

    gce_instance_stop = DeleteGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_stop_task", dag=dag
    )

(start >> gce_instance_start >> fetch_install_code >> [dms_to_gcs_pro, sleep_op])
(
    sleep_op
    >> dms_to_gcs_jeunes
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
