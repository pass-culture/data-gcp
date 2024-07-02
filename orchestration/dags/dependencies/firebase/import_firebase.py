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
        "pc-pro-testing.analytics_397508951",
        "pc-pro-production.analytics_397565568",
    ],
    "stg": [
        "pc-pro-staging.analytics_397573615",
        "pc-pro-production.analytics_397565568",
    ],
    "prod": [
        "pc-pro-production.analytics_397565568",
    ],
}[ENV_SHORT_NAME]


import_firebase_pro_tables = {
    # raw
    "raw_firebase_pro_events": {
        "sql": f"{SQL_PATH}/raw/firebase_pro_events.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "firebase_pro_events",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_name"]},
        "params": {
            "app_info_ids": ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO,
            "gcp_project_native_env": GCP_PROJECT_PRO_ENV,
        },
    },
}

import_firebase_beneficiary_tables = {
    "raw_firebase_events": {
        "sql": f"{SQL_PATH}/raw/firebase_events.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "firebase_events",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_name"]},
        "params": {
            "app_info_ids": ENV_SHORT_NAME_APP_INFO_ID_MAPPING,
            "gcp_project_native_env": GCP_PROJECT_NATIVE_ENV,
        },
    }
}

import_tables = dict(import_firebase_beneficiary_tables, **import_firebase_pro_tables)
