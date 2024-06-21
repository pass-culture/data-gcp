SQL_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql"
SQL_TMP_PATH = SQL_PATH + "/tmp"
SQL_RAW_PATH = SQL_PATH + "/raw"

TMP_TABLES = {
    "past_offer_context": {
        "sql": f"{SQL_TMP_PATH}/past_offer_context.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "past_offer_context_{{ yyyymmdd(today()) }}",
    },
}

RAW_TABLES = {
    "past_offer_context": {
        "sql": f"{SQL_RAW_PATH}/past_offer_context.sql",
        "write_disposition": "WRITE_APPEND",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "past_offer_context${{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "import_date"},
    },
}
