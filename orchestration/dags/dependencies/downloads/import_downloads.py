SQL_ANALYTICS_PATH = f"dependencies/downloads/sql/analytics"
ANALYTICS_TABLES = {
    "app_downloads_stats": {
        "sql": f"{SQL_ANALYTICS_PATH}/app_downloads_stats.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "app_downloads_stats",
    }
}
