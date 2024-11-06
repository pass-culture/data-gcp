SQL_PATH = "dependencies/ml/linkage/sql"
DATE = "{{ yyyymmdd(ds) }}"
PARAMS = {
    "sql": f"{SQL_PATH}/create_product_artist_link_table.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",  # Put the export dataset
    "destination_table": "product_artist_link_table",
}
