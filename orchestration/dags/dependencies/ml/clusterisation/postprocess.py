SQL_PATH = f"dependencies/ml/clusterisation/sql/"
DATE = "{{ yyyymmdd(ds) }}"
postprocess_params = {
    "sql": f"{SQL_PATH}/postprocess_clusters.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_clean_dataset }}",
    "destination_table": f"{DATE}_item_clusters",
}
