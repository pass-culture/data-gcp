from common.config import BIGQUERY_ANALYTICS_DATASET
from dependencies.great_expectations.utils import today, yesterday, last_week

enriched_tables_test_config = {
    "enriched_institution_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "date_field": "last_booking_date",
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
}
