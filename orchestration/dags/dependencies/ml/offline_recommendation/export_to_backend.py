SQL_PATH = "dependencies/ml/offline_recommendation/sql/export_backend"
DATE = "{{ yyyymmdd(ds) }}"
queries = [
    "first_booking",
    "day_plus_two_after_booking",
    "day_plus_fifty_after_deposit",
    "day_plus_thirty_inactivity",
]
params = []
for query in queries:
    params.append(
        {
            "table": f"{query}",
            "sql": f"{SQL_PATH}/{query}.sql",
            "write_disposition": "WRITE_TRUNCATE",
            "destination_dataset": "{{ bigquery_tmp_dataset }}",
            "destination_table": f"{DATE}_{query}_backend",
        }
    )
