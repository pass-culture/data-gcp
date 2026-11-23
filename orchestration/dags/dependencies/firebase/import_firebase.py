from common.config import ENV_SHORT_NAME

SQL_PATH = "dependencies/firebase/sql"


ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "tst": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging", "app.passculture", "app.passculture.webapp"],
    "prod": ["app.passculture", "app.passculture.webapp"],
    "prd": ["app.passculture", "app.passculture.webapp"],
}[ENV_SHORT_NAME]

ENV_SHORT_NAME_APP_INFO_ID_MAPPING_PRO = {
    "dev": [
        "localhost",
        "pro.testing.passculture.team",
        "pro-test.testing.passculture.team",
    ],
    "tst": [
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
    "prd": ["passculture.pro"],
}[ENV_SHORT_NAME]


GCP_PROJECT_NATIVE_DEFAULT_ENV = [
    "pc-native-production.analytics_450774560",
]

GCP_PROJECT_NATIVE_ENV = {
    "dev": GCP_PROJECT_NATIVE_DEFAULT_ENV + ["pc-native-testing.analytics_451612566"],
    "tst": GCP_PROJECT_NATIVE_DEFAULT_ENV + ["pc-native-testing.analytics_451612566"],
    "stg": GCP_PROJECT_NATIVE_DEFAULT_ENV + ["pc-native-staging.analytics_450776578"],
    # keep double run for native while we are migrating to new project.
    "prod": GCP_PROJECT_NATIVE_DEFAULT_ENV + ["passculture-native.analytics_267263535"],
    "prd": GCP_PROJECT_NATIVE_DEFAULT_ENV + ["passculture-native.analytics_267263535"],
}[ENV_SHORT_NAME]


GCP_PROJECT_PRO_DEFAULT_ENV = ["pc-pro-production.analytics_397565568"]

GCP_PROJECT_PRO_ENV = {
    "dev": ["pc-pro-testing.analytics_397508951"] + GCP_PROJECT_PRO_DEFAULT_ENV,
    "tst": ["pc-pro-testing.analytics_397508951"] + GCP_PROJECT_PRO_DEFAULT_ENV,
    "stg": ["pc-pro-staging.analytics_397573615"] + GCP_PROJECT_PRO_DEFAULT_ENV,
    "prod": GCP_PROJECT_PRO_DEFAULT_ENV,
    "prd": GCP_PROJECT_PRO_DEFAULT_ENV,
}[ENV_SHORT_NAME]

# Region of each GA4 / Firebase export (fixed when the export link was created).
# BigQuery cannot query across regions: when a source is not in the same region as
# our datasets (BIGQUERY_LOCATION), the DAG queries it into a staging dataset located
# in the source region, copies the table to our region, then loads it.
FIREBASE_SOURCE_LOCATION = "europe-west1"  # native, pro and performance exports
INSTITUTIONAL_SOURCE_LOCATION = "europe-west9"

# Staging dataset in each source region (created by hand, 1-day table expiration).
BIGQUERY_TMP_DATASET_BY_LOCATION = {
    "europe-west1": f"tmp_eu1_{ENV_SHORT_NAME}",
    "europe-west9": f"tmp_eu9_{ENV_SHORT_NAME}",
}

# No testing / staging project yet: every env imports production data.
GCP_PROJECT_INSTITUTIONAL_DEFAULT_ENV = [
    "pc-site-instit-production.analytics_457326530"
]

GCP_PROJECT_INSTITUTIONAL_ENV = {
    "dev": GCP_PROJECT_INSTITUTIONAL_DEFAULT_ENV,
    "tst": GCP_PROJECT_INSTITUTIONAL_DEFAULT_ENV,
    "stg": GCP_PROJECT_INSTITUTIONAL_DEFAULT_ENV,
    "prod": GCP_PROJECT_INSTITUTIONAL_DEFAULT_ENV,
    "prd": GCP_PROJECT_INSTITUTIONAL_DEFAULT_ENV,
}[ENV_SHORT_NAME]

GCP_PROJECT_PERFORMANCE_ENV = {
    "dev": "pc-native-testing.firebase_performance",
    "tst": "pc-native-testing.firebase_performance",
    "stg": "pc-native-testing.firebase_performance",
    "prod": "pc-native-production.firebase_performance",
    "prd": "pc-native-production.firebase_performance",
}[ENV_SHORT_NAME]


PERFORMANCE_SUFFIX_IOS = {
    "dev": "test_IOS",
    "tst": "test_IOS",
    "stg": "test_IOS",
    "prod": "IOS",
    "prd": "IOS",
}[ENV_SHORT_NAME]


PERFORMANCE_SUFFIX_ANDROID = {
    "dev": "testing_ANDROID",
    "tst": "testing_ANDROID",
    "stg": "testing_ANDROID",
    "prod": "webapp_ANDROID",
    "prd": "webapp_ANDROID",
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
            "gcp_project_env": GCP_PROJECT_PRO_ENV,
        },
        "fallback_params": {"gcp_project_env": GCP_PROJECT_PRO_DEFAULT_ENV},
        "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
        "source_location": FIREBASE_SOURCE_LOCATION,
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
            "gcp_project_env": GCP_PROJECT_NATIVE_ENV,
        },
        "fallback_params": {"gcp_project_env": GCP_PROJECT_NATIVE_DEFAULT_ENV},
        "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
        "source_location": FIREBASE_SOURCE_LOCATION,
    }
}

import_firebase_institutional_tables = {
    "raw_firebase_institutional_events": {
        "sql": f"{SQL_PATH}/raw/firebase_institutional_events.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "firebase_institutional_events",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_name"]},
        "params": {
            "gcp_project_env": GCP_PROJECT_INSTITUTIONAL_ENV,
        },
        "fallback_params": {"gcp_project_env": GCP_PROJECT_INSTITUTIONAL_DEFAULT_ENV},
        "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
        "source_location": INSTITUTIONAL_SOURCE_LOCATION,
    }
}

import_firebase_performance_tables = {
    # raw
    "raw_firebase_ios_performance": {
        "sql": f"{SQL_PATH}/raw/firebase_performance.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "firebase_ios_performance",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_name"]},
        "params": {
            "gcp_project_env": GCP_PROJECT_PERFORMANCE_ENV,
            "suffix": PERFORMANCE_SUFFIX_IOS,
        },
        "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
        "source_location": FIREBASE_SOURCE_LOCATION,
    },
    "raw_firebase_android_performance": {
        "sql": f"{SQL_PATH}/raw/firebase_performance.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "firebase_android_performance",
        "partition_prefix": "$",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_name"]},
        "params": {
            "gcp_project_env": GCP_PROJECT_PERFORMANCE_ENV,
            "suffix": PERFORMANCE_SUFFIX_ANDROID,
        },
        "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"],
        "source_location": FIREBASE_SOURCE_LOCATION,
    },
}

import_tables = dict(
    import_firebase_beneficiary_tables,
    **import_firebase_pro_tables,
    **import_firebase_institutional_tables,
)
import_perf_tables = dict(import_firebase_performance_tables)
