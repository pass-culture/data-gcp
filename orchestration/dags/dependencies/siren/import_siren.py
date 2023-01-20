SQL_ANALYTICS_PATH = f"dependencies/siren/sql/analytics"

ANALYTICS_TABLES = {
    "siren_data": {
        "sql": f"{SQL_ANALYTICS_PATH}/siren_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "siren_data",
    }
}
