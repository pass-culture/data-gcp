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
    "aggregated_daily_user_used_activity": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_user_used_activity.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_daily_user_used_activity",
    },
    "aggregated_monthly_user_used_bookings_activity": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_monthly_user_used_bookings_activity.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_monthly_user_used_bookings_activity",
        "depends" : ["aggregated_daily_user_used_activity"]
    },
}
