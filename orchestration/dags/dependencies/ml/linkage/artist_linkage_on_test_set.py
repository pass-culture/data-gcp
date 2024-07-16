SQL_PATH = "dependencies/ml/linkage/sql"
PARAMS = {
    "sql": f"{SQL_PATH}/artist_linkage_on_test_set.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": "artist_linkage_on_test_set",
}
