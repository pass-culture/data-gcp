SQL_CLEAN_PATH = "dependencies/dms_subscriptions/sql/clean"
CLEAN_TABLES = {
    "dms_jeunes": {
        "sql": f"{SQL_CLEAN_PATH}/dms_cleaned.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "dms_jeunes_cleaned",
        "params": {"target": "jeunes"},
    },
    "dms_pro": {
        "sql": f"{SQL_CLEAN_PATH}/dms_cleaned.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "dms_pro_cleaned",
        "params": {"target": "pro"},
    },
}
