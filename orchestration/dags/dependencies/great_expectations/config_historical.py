from common.config import BIGQUERY_CLEAN_DATASET

historical_applicative_test_config = {
    "applicative_database_booking_history": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "partition_field": "partition_date",
        "nb_days": 7,
    },
    "applicative_database_collective_booking_history": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "partition_field": "partition_date",
        "nb_days": 7,
    },
    "applicative_database_collective_offer_history": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "partition_field": "partition_date",
        "nb_days": 7,
    },
    "applicative_database_collective_offer_template_history": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "partition_field": "partition_date",
        "nb_days": 7,
    },
    "applicative_database_collective_stock_history": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "partition_field": "partition_date",
        "nb_days": 7,
    },
    "applicative_database_offer_history": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "partition_field": "partition_date",
        "nb_days": 7,
    },
    "applicative_database_stock_history": {
        "dataset_name": BIGQUERY_CLEAN_DATASET,
        "partition_field": "partition_date",
        "nb_days": 7,
    },
}
