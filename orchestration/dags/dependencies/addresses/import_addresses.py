USER_LOCATIONS_SCHEMA = [
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_postal_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_department_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "city_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "api_adresse_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "code_epci", "type": "STRING", "mode": "NULLABLE"},
    {"name": "epci_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "qpv_communes", "type": "STRING", "mode": "NULLABLE"},
    {"name": "qpv_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "code_qpv", "type": "STRING", "mode": "NULLABLE"},
    {"name": "zrr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_updated", "type": "DATETIME", "mode": "NULLABLE"},
]

SQL_CLEAN_PATH = "dependencies/addresses/sql/clean"
SQL_ANALYTICS_PATH = "dependencies/addresses/sql/analytics"

CLEAN_TABLES = {
    "user_locations": {
        "sql": f"{SQL_CLEAN_PATH}/user_locations.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "user_locations",
    }
}

ANALYTICS_TABLES = {
    "user_locations": {
        "sql": f"{SQL_ANALYTICS_PATH}/user_locations.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "user_locations",
    }
}
