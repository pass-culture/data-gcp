from common.config import ENV_SHORT_NAME

SQL_PATH = f"dependencies/firebase/sql"


ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging", "app.passculture", "app.passculture.webapp"],
    "prod": ["app.passculture", "app.passculture.webapp"],
}

ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO = {
    "dev": ["localhost", "pro.testing.passculture.team"],
    "stg": [
        "pro.testing.passculture.team",
        "integration.passculture.pro",
        "passculture.pro",
        "pro.staging.passculture.team",
    ],
    "prod": ["passculture.pro"],
}

GCP_PROJECT_NATIVE_ENV = "passculture-native"
FIREBASE_RAW_DATASET = "analytics_267263535"

GCP_PROJECT_PRO_ENV = "passculture-pro"
FIREBASE_PRO_RAW_DATASET = "analytics_301948526"

app_info_id_list = ENV_SHORT_NAME_APP_INFO_ID_MAPPING[ENV_SHORT_NAME]
app_info_id_list_pro = ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO[ENV_SHORT_NAME]


import_firebase_pro_tables = {
    "raw_firebase_pro_events": {
        "sql": f"{SQL_PATH}/raw/events.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "events_pro",
        "partition_prefix": "_",
        "params": {
            "table_type": "pro",
            "app_info_ids": app_info_id_list_pro,
            "gcp_project_native_env": GCP_PROJECT_PRO_ENV,
            "firebase_raw_dataset": FIREBASE_PRO_RAW_DATASET,
        },
    },
    "clean_firebase_pro_events": {
        "sql": f"{SQL_PATH}/clean/events.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "firebase_pro_events",
        "partition_prefix": "_",
        "depends": ["raw_firebase_pro_events"],
        "params": {
            "table_type": "pro",
            "app_info_ids": app_info_id_list_pro,
            "table_name": "events_pro",
        },
    },
    # analytics
    "analytics_firebase_pro_events": {
        "sql": f"{SQL_PATH}/analytics/firebase_pro_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_pro_events",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_name"]},
        "depends": ["clean_firebase_pro_events"],
    },
}

import_firebase_beneficiary_tables = {
    "raw_firebase_events": {
        "sql": f"{SQL_PATH}/raw/events.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "events",
        "partition_prefix": "_",
        "params": {
            "table_type": "beneficiary",
            "app_info_ids": app_info_id_list,
            "gcp_project_native_env": GCP_PROJECT_NATIVE_ENV,
            "firebase_raw_dataset": FIREBASE_RAW_DATASET,
        },
    },
    "clean_firebase_events": {
        "sql": f"{SQL_PATH}/clean/events.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "firebase_events",
        "partition_prefix": "_",
        "depends": ["raw_firebase_events"],
        "params": {
            "table_type": "beneficiary",
            "app_info_ids": app_info_id_list,
            "table_name": "events",
        },
    },
    # analytics
    "analytics_firebase_events": {
        "sql": f"{SQL_PATH}/analytics/firebase_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_events",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_name"]},
        "depends": ["clean_firebase_events"],
    },
    "analytics_firebase_aggregated_offers": {
        "sql": f"{SQL_PATH}/analytics/firebase_aggregated_offers.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_aggregated_offers",
        "depends": ["clean_firebase_events"],
    },
    "analytics_firebase_aggregated_users": {
        "sql": f"{SQL_PATH}/analytics/firebase_aggregated_users.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_aggregated_users",
        "depends": ["clean_firebase_events"],
    },
    "analytics_firebase_visits": {
        "sql": f"{SQL_PATH}/analytics/firebase_visits.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_visits",
        "depends": ["clean_firebase_events"],
    },
    "analytics_firebase_session_origin": {
        "sql": f"{SQL_PATH}/analytics/firebase_session_origin.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_session_origin",
        "time_partitioning": {"field": "first_event_date"},
        "depends": ["analytics_firebase_events"],
    },
    "analytics_firebase_home_events": {
        "sql": f"{SQL_PATH}/analytics/firebase_home_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_events",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_type"]},
        "depends": ["analytics_firebase_events"],
    },
}


import_tables = dict(import_firebase_beneficiary_tables, **import_firebase_pro_tables)
