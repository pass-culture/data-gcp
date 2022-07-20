SQL_PATH = f"dependencies/open_data/sql/open_data"


aggregated_open_data_tables = {
    "aggregated_usage_and_intensity": {
        "sql": f"{SQL_PATH}/aggregated_usage_and_intensity.sql",
        "destination_dataset_table": "{{ bigquery_open_data_project }}.{{ bigquery_open_data_public_dataset }}.aggregated_usage_and_intensity",
    },
    "aggregated_monthly_user_data": {
        "sql": f"{SQL_PATH}/aggregated_monthly_user_data.sql",
        "destination_dataset_table": "{{ bigquery_open_data_project }}.{{ bigquery_open_data_public_dataset }}.aggregated_monthly_user_data",
    },
    "aggregated_monthly_used_booking": {
        "sql": f"{SQL_PATH}/aggregated_monthly_used_booking.sql",
        "destination_dataset_table": "{{ bigquery_open_data_project }}.{{ bigquery_open_data_public_dataset }}.aggregated_monthly_used_booking${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "cluster_fields": ["calculation_month"],
    },
    "raw_supply_data": {
        "sql": f"{SQL_PATH}/raw_supply_data.sql",
        "destination_dataset_table": "{{ bigquery_open_data_project }}.{{ bigquery_open_data_public_dataset }}.raw_supply_data${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "cluster_fields": ["calculation_month"],
    },
}
