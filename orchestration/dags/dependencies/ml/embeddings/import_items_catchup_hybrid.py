SQL_PATH = "dependencies/ml/embeddings/sql"
DATE = "{{ yyyymmdd(ds) }}"
params = {
    "sql": f"{SQL_PATH}/item_to_extract_embedding_catchup_hybrid.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": f"{DATE}_item_to_extract_embeddings_hybrid",
}
