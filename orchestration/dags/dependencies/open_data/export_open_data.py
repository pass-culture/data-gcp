SQL_PATH = f"dependencies/open_data/sql/open_data"


aggregated_open_data_tables = {
    "aggregated_usage_and_intensity": {
        "sql": f"{SQL_PATH}/aggregated_usage_and_intensity.sql",
        "destination_project": "{{ bigquery_open_data_project }}",
        "destination_dataset": "{{ bigquery_open_data_public_dataset }}",
        "destination_table": "aggregated_usage_and_intensity${{ yyyymmdd(current_month(add_days(ds, 1))) }}",
        "time_partitioning": {"field": "calculation_month"},
    },
    "aggregated_monthly_user_data": {
        "sql": f"{SQL_PATH}/aggregated_monthly_user_data.sql",
        "destination_project": "{{ bigquery_open_data_project }}",
        "destination_dataset": "{{ bigquery_open_data_public_dataset }}",
        "destination_table": "aggregated_monthly_user_data",
    },
    "aggregated_monthly_used_booking": {
        "sql": f"{SQL_PATH}/aggregated_monthly_used_booking.sql",
        "destination_project": "{{ bigquery_open_data_project }}",
        "destination_dataset": "{{ bigquery_open_data_public_dataset }}",
        "destination_table": "aggregated_monthly_used_booking",
    },
    "raw_supply_data": {
        "sql": f"{SQL_PATH}/raw_supply_data.sql",
        "destination_project": "{{ bigquery_open_data_project }}",
        "destination_dataset": "{{ bigquery_open_data_public_dataset }}",
        "destination_table": "raw_supply_data${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "clustering_fields": {"fields": ["calculation_month"]},
    },
}
