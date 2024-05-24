SQL_PATH = "dependencies/ml/linkage/sql"
DATE = "{{ yyyymmdd(ds) }}"
table = "artists_to_link"
params = {
    "sql": f"{SQL_PATH}/{table}.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": f"{table}",
}
