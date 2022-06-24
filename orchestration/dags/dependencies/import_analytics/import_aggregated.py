ANALYTICS_SQL_PATH = f"dependencies/import_analytics/sql/analytics"


aggregated_tables = {
    "aggregated_daily_used_booking": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_used_booking.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_daily_used_booking",
    },
    "aggregated_daily_user_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_user_data.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_daily_user_data",
    },
    "aggregated_daily_user_used_activity": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_user_used_activity.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_daily_user_used_activity",
    },
    "aggregated_monthly_user_used_booking_activity": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_monthly_user_used_booking_activity.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.aggregated_monthly_user_used_booking_activity",
        "depends": ["aggregated_daily_user_used_activity"],
    },
}
