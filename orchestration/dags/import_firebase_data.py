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
)
from dependencies.slack_alert import task_fail_slack_alert

ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging"],
    "prod": ["app.passculture"],
}

app_info_id_list = ENV_SHORT_NAME_APP_INFO_ID_MAPPING[ENV_SHORT_NAME]
EXECUTION_DATE = "{{ ds_nodash }}"

default_dag_args = {
    "start_date": datetime.datetime(2021, 4, 13),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}


def _env_switcher():
    next_steps = ["dummy_task_for_branch"]

    if ENV_SHORT_NAME == "prod":
        next_steps.append("copy_table")

    return next_steps


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

env_switcher = BranchPythonOperator(
    task_id="env_switcher",
    python_callable=_env_switcher,
    dag=dag,
)

copy_table = BigQueryOperator(
    task_id=f"copy_table",
    sql=f"SELECT * FROM passculture-native.analytics_267263535.events_"
    + EXECUTION_DATE,
    use_legacy_sql=False,
    destination_dataset_table=f"passculture-data-prod:firebase_raw_data.events_"
    + EXECUTION_DATE,
    write_disposition="WRITE_EMPTY",
    dag=dag,
)
delete_table = BigQueryTableDeleteOperator(
    task_id=f"delete_table",
    deletion_dataset_table=f"passculture-native:analytics_267263535.events_"
    + EXECUTION_DATE,
    ignore_if_missing=True,
    dag=dag,
)

dummy_task_for_branch = DummyOperator(task_id="dummy_task_for_branch", dag=dag)

copy_table_to_env = BigQueryOperator(
    task_id=f"copy_table_to_env",
    sql=f"""
        SELECT * FROM passculture-data-prod.firebase_raw_data.events_{EXECUTION_DATE} WHERE app_info.id IN ({", ".join([f"'{app_info_id}'" for app_info_id in app_info_id_list])})
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.events_{EXECUTION_DATE}",
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

copy_table_to_analytics = BigQueryOperator(
    task_id=f"copy_table_to_analytics",
    sql=f"""
        SELECT * FROM {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.firebase_events_{EXECUTION_DATE}
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.firebase_events_{EXECUTION_DATE}",
    write_disposition="WRITE_TRUNCATE",
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

start >> env_switcher
env_switcher >> dummy_task_for_branch >> copy_table_to_env
env_switcher >> copy_table >> delete_table >> copy_table_to_env
copy_table_to_env >> copy_table_to_clean >> copy_table_to_analytics >> end
copy_table_to_env >> aggregate_firebase_offer_events >> aggregate_firebase_user_events >> end
