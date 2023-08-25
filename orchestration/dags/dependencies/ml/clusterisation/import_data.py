SQL_PATH = f"dependencies/ml/clusterisation/sql/"
DATE = "{{ yyyymmdd(ds) }}"
params = {
    "sql": f"{SQL_PATH}/import_items.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": f"{DATE}_clusterisation_items_raw",
}
