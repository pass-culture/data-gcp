SQL_PATH = "dependencies/ml/offline_recommendation/sql/export_backend"
DATE = "{{ yyyymmdd(ds) }}"
queries = ["first_booking", "day_plus_two_after_booking","day_plus_fifty_deposit","day_plus_thirty_inactif"]
params = []
for query in queries:
    params.append(
        {
            "table": f"{query}",
            "sql": f"{SQL_PATH}/{query}.sql",
            "write_disposition": "WRITE_APPEND",
            "destination_dataset": "{{ bigquery_backend_dataset }}",
            "destination_table": "marketting_offline_recommendation",
            "time_partitioning": {"field": "event_date"},
        }
    )
