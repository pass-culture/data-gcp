from common.config import BIGQUERY_ANALYTICS_DATASET

enriched_tables_test_config = {
    "enriched_booking_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_collective_booking_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_collective_offer_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_deposit_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_institution_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_offer_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_offerer_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_stock_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_suivi_dms_adage": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_user_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_venue_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
    "enriched_reimbursement_data": {
        "dataset_name": BIGQUERY_ANALYTICS_DATASET,
        "freshness_check": "yesterday",
    },
}
