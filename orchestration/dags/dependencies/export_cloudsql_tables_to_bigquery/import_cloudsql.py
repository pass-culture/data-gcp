from common.macros import yesterday
from common.config import BIGQUERY_RAW_DATASET, BIGQUERY_CLEAN_DATASET

SQL_RAW_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/raw"
SQL_CLEAN_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/clean"
SQL_ANALYTICS_PATH = f"dependencies/export_cloudsql_tables_to_bigquery/sql/analytics"


RAW_TABLES = {
    "past_recommended_offers": {
        "sql": f"{SQL_RAW_PATH}/past_recommended_offers.sql",
        "write_disposition": "WRITE_APPEND",
        "params": {
            "yesterday": "{{ yesterday }}"
            },
    },
}

CLEAN_TABLES = {
    "past_recommended_offers": {
        "sql": f"{SQL_CLEAN_PATH}/past_recommended_offers.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "time_partitioning": {"field": "date"},
        "cluster_fields": ["date"],
    },
}

ANALYTICS_TABLES = {
    "past_recommended_offers": {
        "sql": f"{SQL_ANALYTICS_PATH}/past_recommended_offers.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "time_partitioning": {"field": "date"},
        "cluster_fields": ["date"],
    },
}
