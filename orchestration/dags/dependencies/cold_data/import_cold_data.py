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
}
