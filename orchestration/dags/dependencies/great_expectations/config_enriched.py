from common.config import BIGQUERY_ANALYTICS_DATASET
from dependencies.great_expectations.utils import last_week, today, yesterday

enriched_tables_test_config = {
    "enriched_collective_booking_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "collective_booking_creation_date",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
    "enriched_collective_offer_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "collective_offer_creation_date",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
    "enriched_deposit_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "deposit_creation_date",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
    "enriched_institution_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "last_booking_date",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
    "enriched_offerer_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "offerer_creation_date",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
    "enriched_stock_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "stock_creation_date",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
    "enriched_suivi_dms_adage": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "processed_at",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
    "enriched_user_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "user_activation_date",
        "freshness_check": {
            "dev": [last_week, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },
}
