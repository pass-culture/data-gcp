from common.config import BIGQUERY_RAW_DATASET, BIGQUERY_TMP_DATASET

SQL_PATH = "dependencies/export_cloudsql_tables_to_bigquery/sql"
SQL_TMP_PATH = SQL_PATH + "/tmp"
SQL_RAW_PATH = SQL_PATH + "/raw"

PAST_OFFER_CONTEXT_TMP_QUERY = {
    "sql": f"{SQL_TMP_PATH}/past_offer_context.sql",
    "write_disposition": "WRITE_TRUNCATE",
    "destination_dataset": BIGQUERY_TMP_DATASET,
    "destination_table": "past_offer_context_{{ yyyymmdd(ds) }}",
}

PAST_OFFER_CONTEXT_RAW_QUERY = {
    "sql": f"{SQL_RAW_PATH}/past_offer_context.sql",
    "write_disposition": "WRITE_APPEND",
    "destination_dataset": BIGQUERY_RAW_DATASET,
    "destination_table": "past_offer_context${{ yyyymmdd(ds) }}",
    "time_partitioning": {"field": "import_date"},
}
