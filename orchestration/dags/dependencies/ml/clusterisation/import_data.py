SQL_PATH = f"dependencies/ml/clusterisation/sql/"

IMPORT_ITEM_EMBEDDINGS = {
    "sql": f"{SQL_PATH}/import_item_embeddings.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": "{{ yyyymmdd(ds) }}_import_item_embeddings",
}
