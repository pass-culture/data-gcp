import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
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
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    DAG_FOLDER,
)

from dependencies.dms_subscriptions.import_dms_subscriptions import (
    parse_api_result,
    ANALYTICS_TABLES,
)
from common import macros
from common.utils import getting_service_account_token, get_airflow_schedule


DMS_FUNCTION_NAME = "dms_" + ENV_SHORT_NAME

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
    dagrun_timeout=timedelta(minutes=180),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "updated_since": Param(
            default="2023-01-01" if ENV_SHORT_NAME == "dev" else "2019-01-01"
        )
    },
) as dag:

    start = DummyOperator(task_id="start")

    getting_service_account_token = PythonOperator(
        task_id="getting_service_account_token",
        python_callable=getting_service_account_token,
        op_kwargs={"function_name": f"{DMS_FUNCTION_NAME}"},
    )

    dms_to_gcs = SimpleHttpOperator(
        task_id="dms_to_gcs",
        method="POST",
        http_conn_id="http_gcp_cloud_function",
        endpoint=DMS_FUNCTION_NAME,
        data=json.dumps({"updated_since": "{{ params.updated_since }}"}),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
        },
        log_response=True,
        do_xcom_push=True,
    )

    parse_api_result_jeunes = PythonOperator(
        task_id="parse_api_result_jeunes",
        python_callable=parse_api_result,
        op_args=[
            "{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}",
            "jeunes",
        ],
        dag=dag,
    )

    parse_api_result_pro = PythonOperator(
        task_id="parse_api_result_pro",
        python_callable=parse_api_result,
        op_args=[
            "{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}",
            "pro",
        ],
        dag=dag,
    )

    import_dms_jeunes_to_bq = GCSToBigQueryOperator(
        task_id="import_dms_jeunes_to_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "dms_export/dms_jeunes_{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}.parquet"
        ],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.dms_jeunes",
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
        ],
        write_disposition="WRITE_APPEND",
    )

    import_dms_pro_to_bq = GCSToBigQueryOperator(
        task_id="import_dms_pro_to_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "dms_export/dms_pro_{{task_instance.xcom_pull(task_ids='dms_to_gcs', key='return_value')}}.parquet"
        ],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.dms_pro",
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
        ],
        write_disposition="WRITE_APPEND",
    )

    analytics_tasks = []
    for table, params in ANALYTICS_TABLES.items():
        task = bigquery_job_task(table=table, dag=dag, job_params=params)
        analytics_tasks.append(task)

    end = DummyOperator(task_id="end")


(
    start
    >> getting_service_account_token
    >> dms_to_gcs
    >> [parse_api_result_jeunes, parse_api_result_pro]
)

parse_api_result_jeunes >> import_dms_jeunes_to_bq >> analytics_tasks[0] >> end
parse_api_result_pro >> import_dms_pro_to_bq >> analytics_tasks[1] >> end
