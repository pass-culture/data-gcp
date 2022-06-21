ANALYTICS_SQL_PATH = f"dependencies/import_analytics/sql/analytics"


aggregated_tables = {
    "aggregated_daily_used_bookings": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_used_bookings.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_daily_used_bookings",
    },
    "aggregated_daily_user_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_user_data.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_daily_user_data",
    },
}
