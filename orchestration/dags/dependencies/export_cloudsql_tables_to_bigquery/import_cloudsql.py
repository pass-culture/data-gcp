SQL_RAW_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/raw"
SQL_CLEAN_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/clean"
SQL_ANALYTICS_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/analytics"


RAW_TABLES = {
    "past_recommended_offers": {
        "sql": f"{SQL_RAW_PATH}/past_recommended_offers.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "past_recommended_offers",
    }
}

CLEAN_TABLES = {
    "past_recommended_offers": {
        "sql": f"{SQL_CLEAN_PATH}/past_recommended_offers.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "time_partitioning": {"field": "date"},
        "cluster_fields": ["date"],
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "past_recommended_offers",
    }
}
