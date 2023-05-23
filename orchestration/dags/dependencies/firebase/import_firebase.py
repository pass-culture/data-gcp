from common.config import ENV_SHORT_NAME

SQL_PATH = f"dependencies/firebase/sql"


ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging", "app.passculture", "app.passculture.webapp"],
    "prod": ["app.passculture", "app.passculture.webapp"],
}[ENV_SHORT_NAME]

ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO = {
    "dev": [
        "localhost",
        "pro.testing.passculture.team",
        "pro-test.testing.passculture.team",
    ],
    "stg": [
        "pro.testing.passculture.team",
        "integration.passculture.pro",
        "passculture.pro",
        "pro.staging.passculture.team",
    ],
    "prod": ["passculture.pro"],
}[ENV_SHORT_NAME]

GCP_PROJECT_NATIVE_ENV = {
    "dev": ["passculture-native.analytics_267263535"],
    "stg": ["passculture-native.analytics_267263535"],
    "prod": ["passculture-native.analytics_267263535"],
}[ENV_SHORT_NAME]


GCP_PROJECT_PRO_ENV = {
    "dev": [
        "passculture-pro.analytics_301948526",
        "pc-pro-testing.analytics_355536579",
    ],
    "stg": ["passculture-pro.analytics_301948526"],
    "prod": ["passculture-pro.analytics_301948526"],
}[ENV_SHORT_NAME]


import_firebase_pro_tables = {
    "raw_firebase_pro_events": {
        "sql": f"{SQL_PATH}/raw/events.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "events_pro",
        "partition_prefix": "_",
        "params": {
            "table_type": "pro",
            "app_info_ids": ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO,
            "gcp_project_native_env": GCP_PROJECT_PRO_ENV,
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
            "app_info_ids": ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO,
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
            "app_info_ids": ENV_SHORT_NAME_APP_INFO_ID_MAPPING,
            "gcp_project_native_env": GCP_PROJECT_NATIVE_ENV,
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
            "app_info_ids": ENV_SHORT_NAME_APP_INFO_ID_MAPPING,
            "table_name": "events",
        },
    },
    "clean_firebase_app_experiments": {
        "sql": f"{SQL_PATH}/clean/firebase_app_experiments.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "firebase_app_experiments",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "depends": ["raw_firebase_events"],
        "params": {
            "table_type": "beneficiary",
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
    "analytics_firebase_home_events": {
        "sql": f"{SQL_PATH}/analytics/firebase_home_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_events",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_type"]},
        "depends": ["analytics_firebase_events"],
    },
    "analytics_firebase_session_origin": {
        "sql": f"{SQL_PATH}/analytics/firebase_session_origin.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_session_origin",
        "time_partitioning": {"field": "first_event_date"},
        "depends": ["analytics_firebase_events"],
    },
    "analytics_firebase_similar_offer_events": {
        "sql": f"{SQL_PATH}/analytics/firebase_similar_offer_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_similar_offer_events",
        "time_partitioning": {"field": "event_date"},
        "depends": ["analytics_firebase_events"],
    },
    "analytics_firebase_booking_origin": {
        "sql": f"{SQL_PATH}/analytics/firebase_booking_origin.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_booking_origin",
        "partition_prefix": "$",
        "time_partitioning": {"field": "booking_date"},
        "dag_depends": ["import_contentful"],
    },
    "analytics_firebase_home_funnel_conversion": {
        "sql": f"{SQL_PATH}/analytics/firebase_home_funnel_conversion.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_funnel_conversion",
        "partition_prefix": "$",
        "time_partitioning": {"field": "module_displayed_date"},
        "dag_depends": ["import_contentful"],
    },
    "analytics_firebase_bookings": {
        "sql": f"{SQL_PATH}/analytics/firebase_bookings.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "analytics_firebase_bookings",
        "partition_prefix": "$",
        "time_partitioning": {"field": "booking_date"},
    },
    "analytics_firebase_app_experiments": {
        "sql": f"{SQL_PATH}/analytics/firebase_app_experiments.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_app_experiments",
        "depends": ["clean_firebase_app_experiments"],
    },
}

import_tables = dict(import_firebase_beneficiary_tables, **import_firebase_pro_tables)
