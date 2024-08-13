SQL_PATH = "dependencies/gcs_seed/sql"

# TODO: legacy code, should be removed or refactored within DBT.

ANALYTICS_TABLES = {
    "departmental_objectives": {
        "sql": f"{SQL_PATH}/analytics/departmental_objectives.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "departmental_objectives",
    },
    "eac_cash_in": {
        "sql": f"{SQL_PATH}/analytics/eac_cash_in.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "eac_cash_in",
    },
    "institutional_partners": {
        "sql": f"{SQL_PATH}/analytics/institutional_partners.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "institutional_partners",
    },
    "pilote_geographic_standards": {
        "sql": f"{SQL_PATH}/analytics/pilote_geographic_standards.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "pilote_geographic_standards",
    },
}
