BACKEND_SQL_PATH = f"dependencies/backend/sql"

create_tables = {
    "favorites_not_booked": {
        "sql": f"{BACKEND_SQL_PATH}/favorites_not_booked.sql",
        "destination_dataset_table": "{{ bigquery_backend_dataset }}.favorites_not_booked${{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "weekly",
    },
    "users_that_landed_on_come_back_later": {
        "sql": f"{BACKEND_SQL_PATH}/users_that_landed_on_come_back_later.sql",
        "destination_dataset_table": "{{ bigquery_backend_dataset }}.users_that_landed_on_come_back_later",
        "schedule_type": "daily",
    },
}
