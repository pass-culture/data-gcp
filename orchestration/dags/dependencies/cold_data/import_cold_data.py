from common.config import DAG_FOLDER

SQL_PATH = f"dependencies/cold_data/sql"

analytics_tables = {
    "macro_rayons": {
        "sql": f"{SQL_PATH}/analytics/macro_rayons.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "macro_rayons",
    },
    "eac_cash_in": {
        "sql": f"{SQL_PATH}/analytics/eac_cash_in.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "eac_cash_in",
    },
    "titelive_isbn_weight": {
        "sql": f"{SQL_PATH}/analytics/titelive_isbn_weight.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "titelive_isbn_weight",
    },
    "institutional_partners": {
        "sql": f"{SQL_PATH}/analytics/institutional_partners.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "institutional_partners",
    },
    "festival_increments": {
        "sql": f"{SQL_PATH}/analytics/festival_increments.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "festival_increments",
    },
    "departmental_objectives": {
        "sql": f"{SQL_PATH}/analytics/departmental_objectives.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "departmental_objectives",
    },
    "agg_partner_cultural_sector": {
        "sql": f"{SQL_PATH}/analytics/agg_partner_cultural_sector.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "agg_partner_cultural_sector",
    },
    "pilote_geographic_standards": {
        "sql": f"{SQL_PATH}/analytics/pilote_geographic_standards.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "pilote_geographic_standards",
    },
}
