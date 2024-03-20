SQL_PATH = f"dependencies/views/sql"


import_tables = {
    "backend_events": {
        "sql": f"{SQL_PATH}/analytics/backend_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "backend_events",
    },
    "clean_api_logs": {
        "sql": f"{SQL_PATH}/clean/api_logs.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "api_logs",
    },
    "analytics_api_logs": {
        "sql": f"{SQL_PATH}/analytics/api_logs.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "api_logs",
    },
}
