APPSFLYER_SQL_PATH = f"dependencies/appsflyer/sql/analytics"

analytics_tables = {
    "appsflyer_users": {
        "sql": f"{APPSFLYER_SQL_PATH}/appsflyer_users.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_users",
    },
    "appsflyer_install_report": {
        "sql": f"{APPSFLYER_SQL_PATH}/appsflyer_install_report.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_install_report",
        "depends": ["appsflyer_users"],
    },
    "appsflyer_daily_report": {
        "sql": f"{APPSFLYER_SQL_PATH}/appsflyer_daily_report.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "appsflyer_daily_report",
    },
}
