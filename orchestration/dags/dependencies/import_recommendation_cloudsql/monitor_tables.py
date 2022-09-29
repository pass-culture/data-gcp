SQL_PATH = f"dependencies/import_recommendation_cloudsql/monitoring_queries"

monitoring_tables = {
    "recommendable_offers_data": {
        "sql": f"{SQL_PATH}/monitor_recommendable_offers_data.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
    }
}
