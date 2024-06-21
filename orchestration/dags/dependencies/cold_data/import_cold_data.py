from common.config import DAG_FOLDER

SQL_PATH = f"dependencies/cold_data/sql"

clean_tables = {
    "institutional_scholar_level": {
        "sql": f"{SQL_PATH}/clean/institutional_scholar_level.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "institutional_scholar_level",
    },
    "eac_webinar": {
        "sql": f"{SQL_PATH}/clean/eac_webinar.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "eac_webinar",
    },
    "iris_france": {
        "sql": f"{SQL_PATH}/clean/iris_france.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "iris_france",
    },
    "geo_iris": {
        "sql": f"{SQL_PATH}/clean/geo_iris.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "geo_iris",
    },
}

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
    "priority_local_authorities": {
        "sql": f"{SQL_PATH}/analytics/priority_local_authorities.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "priority_local_authorities",
    },
    "eple": {
        "sql": f"{SQL_PATH}/analytics/eple.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "eple",
    },
    "rural_city_type_data": {
        "sql": f"{SQL_PATH}/analytics/rural_city_type_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "rural_city_type_data",
    },
}

import_tables = dict(clean_tables, **analytics_tables)
