APPSFLYER_CLEAN_SQL_PATH = f"dependencies/appsflyer/sql/clean"
APPSFLYER_ANALYTICS_SQL_PATH = f"dependencies/appsflyer/sql/analytics"

dag_tables = {
    "appsflyer_install_report": {
        "sql": f"{APPSFLYER_ANALYTICS_SQL_PATH}/appsflyer_install_report.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_install_report",
    },
    "appsflyer_daily_report": {
        "sql": f"{APPSFLYER_ANALYTICS_SQL_PATH}/appsflyer_daily_report.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_daily_report",
    },
}
