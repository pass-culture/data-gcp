SQL_RAW_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/raw"
SQL_CLEAN_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/clean"

RAW_TABLES = {
    "past_recommended_offers": {
        "sql": f"{SQL_RAW_PATH}/past_recommended_offers.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "past_recommended_offers",
    },
    "past_similar_offers": {
        "sql": f"{SQL_RAW_PATH}/past_similar_offers.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "past_similar_offers",
    },
    "past_offer_context": {
        "sql": f"{SQL_RAW_PATH}/past_offer_context.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "past_offer_context",
    },
}

CLEAN_TABLES = {
    "past_recommended_offers": {
        "sql": f"{SQL_CLEAN_PATH}/past_recommended_offers.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "time_partitioning": {"field": "date"},
        "clustering_fields": {"fields": ["date"]},
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "past_recommended_offers",
    },
    "past_similar_offers": {
        "sql": f"{SQL_CLEAN_PATH}/past_similar_offers.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_date"]},
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "past_similar_offers",
    },
    "past_offer_context": {
        "sql": f"{SQL_CLEAN_PATH}/past_offer_context.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_date"]},
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "past_offer_context",
    },
}
