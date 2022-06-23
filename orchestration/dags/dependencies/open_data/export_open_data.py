SQL_PATH = f"dependencies/open_date/sql/open_data"


aggregated_open_data_tables = {
    "aggregated_usage_and_intensity": {
        "sql": f"{SQL_PATH}/aggregated_usage_and_intensity.sql",
        "destination_dataset_table": "{{ bigquery_open_data_public_dataset }}.aggregated_usage_and_intensity",
    },
    "aggregated_monthly_user_data": {
        "sql": f"{SQL_PATH}/aggregated_monthly_user_data.sql",
        "destination_dataset_table": "{{ bigquery_open_data_public_dataset }}.aggregated_monthly_user_data",
    },
    "aggregated_monthly_user_booking": {
        "sql": f"{SQL_PATH}/aggregated_monthly_user_booking.sql",
        "destination_dataset_table": "{{ bigquery_open_data_public_dataset }}.aggregated_monthly_user_booking",
    },
}
