from common.config import (
    ENV_SHORT_NAME,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    DAG_FOLDER,
)

CLEAN_SQL_PATH = f"dependencies/ml/clusterisation/sql/"

raw_tables = {
    "items": {
        "sql": f"{CLEAN_SQL_PATH}/import_items.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "clusterisation_items_raw",
    },
}
