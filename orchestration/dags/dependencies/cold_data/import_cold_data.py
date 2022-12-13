from common.config import DAG_FOLDER

SQL_PATH = f"dependencies/cold_data/sql"

analytics_tables = {
    "macro_rayons": {
        "sql": f"{SQL_PATH}/analytics/macro_rayons.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.macro_rayons",
    },
}
