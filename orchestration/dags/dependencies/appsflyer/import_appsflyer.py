APPSFLYER_CLEAN_SQL_PATH = f"dependencies/appsflyer/sql/clean"
APPSFLYER_ANALYTICS_SQL_PATH = f"dependencies/appsflyer/sql/analytics"

dag_tables = {
    "clean_appsflyer_users": {
        "sql": f"{APPSFLYER_CLEAN_SQL_PATH}/appsflyer_users.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "appsflyer_users",
    },
    "analytics_appsflyer_users": {
        "sql": f"{APPSFLYER_ANALYTICS_SQL_PATH}/appsflyer_users.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_users",
        "depends": ["clean_appsflyer_users"],
    },
    "appsflyer_install_report": {
        "sql": f"{APPSFLYER_ANALYTICS_SQL_PATH}/appsflyer_install_report.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_install_report",
        "depends": ["analytics_appsflyer_users"],
    },
    "appsflyer_daily_report": {
        "sql": f"{APPSFLYER_ANALYTICS_SQL_PATH}/appsflyer_daily_report.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_daily_report",
    },
}
