import datetime
from common import macros
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator

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
    copy_pro_to_analytics,
)
from common.alerts import task_fail_slack_alert

ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging"],
    "prod": ["app.passculture", "app.passculture.webapp"],
}

ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO = {
    "dev": ["localhost", "pro.testing.passculture.team"],
    "stg": ["pro.testing.passculture.team", "integration.passculture.pro"],
    "prod": ["passculture.pro"],
}

GCP_PROJECT_NATIVE_ENV = "passculture-native"
FIREBASE_RAW_DATASET = "analytics_267263535"

GCP_PROJECT_PRO_ENV = "passculture-pro"
FIREBASE_PRO_RAW_DATASET = "analytics_301948526"

app_info_id_list = ENV_SHORT_NAME_APP_INFO_ID_MAPPING[ENV_SHORT_NAME]
app_info_id_list_pro = ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO[ENV_SHORT_NAME]


dags = {
    "full": {
        "prefix": "_",
        "schedule_interval": "00 13 * * *",
        # two days ago
        "yyyymmdd": "{{ yyyymmdd(add_days(ds, -1)) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 6,
            "retry_delay": datetime.timedelta(hours=6),
            "project_id": GCP_PROJECT,
        },
    },
    "intraday": {
        "prefix": "_intraday_",
        "schedule_interval": "00 01 * * *",
        # one day ago
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 1,
            "retry_delay": datetime.timedelta(hours=6),
            "project_id": GCP_PROJECT,
        },
    },
}


for type, params in dags.items():
    dag_id = f"import_{type}_firebase_data_v3"
    prefix = params["prefix"]
    yyyymmdd = params["yyyymmdd"]

    dag = DAG(
        dag_id,
        default_args=params["default_dag_args"],
        description="Import firebase data and dispatch it to each env",
        on_failure_callback=task_fail_slack_alert,
        schedule_interval=params["schedule_interval"],
        catchup=True,
        dagrun_timeout=datetime.timedelta(minutes=90),
        user_defined_macros=macros.default,
    )

    globals()[dag_id] = dag

    start = DummyOperator(task_id="start", dag=dag)

    copy_table_to_env = BigQueryExecuteQueryOperator(
        task_id="copy_table_to_env",
        sql=f"""
            SELECT * FROM {GCP_PROJECT_NATIVE_ENV}.{FIREBASE_RAW_DATASET}.events{prefix}{yyyymmdd} WHERE app_info.id IN ({", ".join([f"'{app_info_id}'" for app_info_id in app_info_id_list])}) OR app_info.id is NULL
            """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.events_{yyyymmdd}",
        write_disposition="WRITE_EMPTY",
        trigger_rule="none_failed",
        dag=dag,
    )

    import_table_pro_to_raw = BigQueryExecuteQueryOperator(
        task_id="import_pro_to_raw",
        sql=f"""
            SELECT * FROM {GCP_PROJECT_PRO_ENV}.{FIREBASE_PRO_RAW_DATASET}.events{prefix}{yyyymmdd} WHERE device.web_info.hostname IN ({", ".join([f"'{app_info_id}'" for app_info_id in app_info_id_list_pro])})
            """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.firebase_pro_{yyyymmdd}",
        write_disposition="WRITE_EMPTY",
        trigger_rule="none_failed",
        dag=dag,
    )

    copy_table_to_clean = BigQueryExecuteQueryOperator(
        task_id="copy_table_to_clean",
        sql=f"""
            SELECT * FROM {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.events_{yyyymmdd}
            """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.firebase_events_{yyyymmdd}",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    copy_table_pro_to_clean = BigQueryExecuteQueryOperator(
        task_id="copy_table_pro_to_clean",
        sql=f"""
            SELECT * FROM {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.firebase_pro_{yyyymmdd}
            """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.firebase_pro_events_{yyyymmdd}",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    copy_firebase_table_to_analytics = BigQueryExecuteQueryOperator(
        task_id="copy_firebase_table_to_analytics",
        sql=copy_table_to_analytics(
            gcp_project=GCP_PROJECT,
            bigquery_raw_dataset=BIGQUERY_RAW_DATASET,
            table_name="events",
            yyyymmdd=yyyymmdd,
        ),
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.firebase_events${ yyyymmdd }",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    copy_table_pro_to_analytics = BigQueryExecuteQueryOperator(
        task_id="copy_table_pro_to_analytics",
        sql=copy_pro_to_analytics(
            gcp_project=GCP_PROJECT,
            bigquery_raw_dataset=BIGQUERY_RAW_DATASET,
            table_name="firebase_pro",
            yyyymmdd=yyyymmdd,
        ),
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.firebase_pro_events${ yyyymmdd }",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    aggregate_firebase_visits_job = BigQueryExecuteQueryOperator(
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

    aggregate_firebase_offer_events_job = BigQueryExecuteQueryOperator(
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

    aggregate_firebase_user_events_job = BigQueryExecuteQueryOperator(
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

    (
        start
        >> copy_table_to_env
        >> copy_table_to_clean
        >> copy_firebase_table_to_analytics
        >> end
    )
    (
        start
        >> import_table_pro_to_raw
        >> copy_table_pro_to_clean
        >> copy_table_pro_to_analytics
        >> end
    )
    (
        copy_table_to_env
        >> [
            aggregate_firebase_visits_job,
            aggregate_firebase_offer_events_job,
            aggregate_firebase_user_events_job,
        ]
        >> end
    )
