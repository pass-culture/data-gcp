import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from dependencies.config import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)
from dependencies.data_analytics.enriched_data.enriched_firebase import (
    aggregate_firebase_user_events,
    aggregate_firebase_offer_events,
    aggregate_firebase_visits,
    copy_table_to_analytics,
)
from dependencies.slack_alert import task_fail_slack_alert

ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging"],
    "prod": ["app.passculture", "app.passculture.webapp"],
}

ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO = {
    "dev": ["localhost", "pro.testing.passculture.team"],
    "stg": ["pro.testing.passculture.team","integration.passculture.pro"],
    "prod": ["passculture.pro"],
}

GCP_PROJECT_NATIVE_ENV = "passculture-native"
FIREBASE_RAW_DATASET = "analytics_267263535"

GCP_PROJECT_PRO_ENV = "passculture-pro"
FIREBASE_PRO_RAW_DATASET = "analytics_301948526"

app_info_id_list = ENV_SHORT_NAME_APP_INFO_ID_MAPPING[ENV_SHORT_NAME]
app_info_id_list_pro = ENV_SHORT_NAME_APP_INFO_ID_MAPPING[ENV_SHORT_NAME]
EXECUTION_DATE = "{{ ds_nodash }}"

default_dag_args = {
    "start_date": datetime.datetime(2021, 4, 17),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}


dag = DAG(
    "import_firebase_data_v3",
    default_args=default_dag_args,
    description="Import firebase data and dispatch it to each env",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 12 * * *" if ENV_SHORT_NAME == "prod" else "30 12 * * *",
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)


copy_table_to_env = BigQueryOperator(
    task_id="copy_table_to_env",
    sql=f"""
        SELECT * FROM {GCP_PROJECT_NATIVE_ENV}.{FIREBASE_RAW_DATASET}.events_{EXECUTION_DATE} WHERE app_info.id IN ({", ".join([f"'{app_info_id}'" for app_info_id in app_info_id_list])}) OR app_info.id is NULL
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.events_{EXECUTION_DATE}",
    write_disposition="WRITE_EMPTY",
    trigger_rule="none_failed",
    dag=dag,
)

import_table_pro_to_raw = BigQueryOperator(
    task_id="import_pro_to_raw",
    sql=f"""
        SELECT * FROM {GCP_PROJECT_PRO_ENV}.{FIREBASE_PRO_RAW_DATASET}.events_{EXECUTION_DATE} WHERE device.web_info.hostname IN ({", ".join([f"'{app_info_id}'" for app_info_id in app_info_id_list_pro])})
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.events_pro_{EXECUTION_DATE}",
    write_disposition="WRITE_EMPTY",
    trigger_rule="none_failed",
    dag=dag,
)

copy_table_to_clean = BigQueryOperator(
    task_id="copy_table_to_clean",
    sql=f"""
        SELECT * FROM {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.events_{EXECUTION_DATE}
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.firebase_events_{EXECUTION_DATE}",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

copy_table_pro_to_clean = BigQueryOperator(
    task_id="copy_table_pro_to_clean",
    sql=f"""
        SELECT * FROM {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.events_pro_{EXECUTION_DATE}
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.firebase_pro_events_{EXECUTION_DATE}",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

copy_table_to_analytics = BigQueryOperator(
    task_id="copy_table_to_analytics",
    sql=copy_table_to_analytics(
        gcp_project=GCP_PROJECT,
        bigquery_raw_dataset=BIGQUERY_RAW_DATASET,
        execution_date=EXECUTION_DATE,
    ),
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.firebase_events",
    write_disposition="WRITE_APPEND",
    dag=dag,
)

copy_table_pro_to_analytics = BigQueryOperator(
    task_id="copy_table_pro_to_analytics",
    sql=f"""
         SELECT * FROM {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.firebase_pro_events_{EXECUTION_DATE}
         """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.firebase_events",
    write_disposition="WRITE_APPEND",
    dag=dag,
)

aggregate_firebase_visits = BigQueryOperator(
    task_id="aggregate_firebase_visits",
    sql=aggregate_firebase_visits(
        gcp_project=GCP_PROJECT,
        bigquery_raw_dataset=BIGQUERY_RAW_DATASET,
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.firebase_visits",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

aggregate_firebase_offer_events = BigQueryOperator(
    task_id="aggregate_firebase_offer_events",
    sql=aggregate_firebase_offer_events(
        gcp_project=GCP_PROJECT,
        bigquery_raw_dataset=BIGQUERY_RAW_DATASET,
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.firebase_aggregated_offers",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

aggregate_firebase_user_events = BigQueryOperator(
    task_id="aggregate_firebase_user_events",
    sql=aggregate_firebase_user_events(
        gcp_project=GCP_PROJECT,
        bigquery_raw_dataset=BIGQUERY_RAW_DATASET,
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.firebase_aggregated_users",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)


end = DummyOperator(task_id="end", dag=dag)

start >> copy_table_to_env >> copy_table_to_clean >> copy_table_to_analytics >> end
start >> import_table_pro_to_raw >> copy_table_pro_to_clean >> copy_table_pro_to_analytics >> end
(
    copy_table_to_env
    >> [
        aggregate_firebase_visits,
        aggregate_firebase_offer_events,
        aggregate_firebase_user_events,
    ]
    >> end
)
