from common.config import BIGQUERY_CLEAN_DATASET
from dependencies.great_expectations.utils import today, yesterday, last_week

clean_tables_test_config = {
    "firebase_events": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "date_field": "",
        "freshness_check": {
            "dev": [yesterday, today],
            "stg": [yesterday, today],
            "prod": [yesterday, today],
        },
    },

}
