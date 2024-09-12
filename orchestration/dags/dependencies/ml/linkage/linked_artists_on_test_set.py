SQL_PATH = "dependencies/ml/linkage/sql"
PARAMS = {
    "sql": f"{SQL_PATH}/linked_artists_on_test_set.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": "{{ bigquery_tmp_dataset }}",
    "destination_table": "linked_artists_on_test_set",
}
