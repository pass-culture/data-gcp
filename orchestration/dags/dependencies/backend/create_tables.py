BACKEND_SQL_PATH = f"dependencies/backend/sql"

create_tables = {
    "favorites_not_booked": {
        "sql": f"{BACKEND_SQL_PATH}/favorites_not_booked.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "favorites_not_booked${{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "weekly",
        "write_disposition": "WRITE_APPEND",
    },
    "churned_40_days_ago": {
        "sql": f"{BACKEND_SQL_PATH}/churned_40_days_ago.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "churned_40_days_ago{{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "daily",
        "write_disposition": "WRITE_APPEND",
    },
    "last_booking_40_days_ago": {
        "sql": f"{BACKEND_SQL_PATH}/last_booking_40_days_ago.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "last_booking_40_days_ago{{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "daily",
        "write_disposition": "WRITE_APPEND",
    },
}
