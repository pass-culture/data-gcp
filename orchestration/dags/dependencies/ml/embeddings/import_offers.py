SQL_PATH = "dependencies/ml/embeddings/sql"
params = {
    "sql": f"{SQL_PATH}/offer_to_extract_embedding.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": "offer_to_extract_embeddings",
}
