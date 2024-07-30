SQL_PATH = "dependencies/ml/linkage/sql"
DATE = "{{ yyyymmdd(ds) }}"
TMP_DATASET = "{{ bigquery_tmp_dataset }}"
ANALYTICS_DATASET = "{{ bigquery_analytics_dataset }}"
PREPROCESS_INPUT_TABLE = (f"{DATE}_items_to_link",)
PREPROCESS_OUTPUT_TABLE = f"{DATE}_items_to_link_clean"
MAIN_OUTPUT_TABLE = f"{DATE}_linked_offers_full"
POSTPROCESS_OUTPUT_TABLE = f"{DATE}_linked_offers_full_postprocessed"

SQL_IMPORT_PARAMS = {
    "sql": f"{SQL_PATH}/items_to_link.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": TMP_DATASET,
    "destination_table": PREPROCESS_INPUT_TABLE,
}
