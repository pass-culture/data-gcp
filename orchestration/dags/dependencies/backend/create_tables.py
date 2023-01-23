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
    "users_that_landed_on_come_back_later": {
        "sql": f"{BACKEND_SQL_PATH}/users_that_landed_on_come_back_later.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "users_that_landed_on_come_back_later",
        "schedule_type": "daily",
        "write_disposition": "WRITE_APPEND",
    },
}
