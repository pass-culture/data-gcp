from common.config import DAG_FOLDER

SQL_PATH = f"dependencies/cold_data/sql"

analytics_tables = {
    "macro_rayons": {
        "sql": f"{SQL_PATH}/analytics/macro_rayons.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.macro_rayons",
    },
    "eac_cash_in": {
        "sql": f"{SQL_PATH}/analytics/eac_cash_in.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.eac_cash_in",
    },
    "titelive_isbn_weight_20230208": {
        "sql": f"{SQL_PATH}/analytics/titelive_isbn_weight_20230208.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.titelive_isbn_weight_20230208",
    },
}
