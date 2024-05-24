SQL_PATH = "dependencies/ml/linkage/sql"
DATE = "{{ yyyymmdd(ds) }}"
PARAMS = {
    "sql": f"{SQL_PATH}/artists_to_link.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": "artists_to_link",
}
