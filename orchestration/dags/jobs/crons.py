SCHEDULE_DICT = {
    "algo_training_two_towers": {
        "prod": "0 12 * * 4",
        "dev": "0 12 * * 2",
        "stg": "0 12 * * 3",
    },
    "artist_linkage": "0 12 * * 0",
    "bigquery_export_old_partitions": "0 7 * * *",
    "export_clickhouse_daily": {
        "prod": "0 6 * * *",
        "stg": "45 4 * * *",
        "dev": "45 4 * * *",
    },
    "dbt_artifacts": "0 6 * * *",
    "dbt_monthly": "30 10 1 * *",
    "dbt_run_dag": "45 2 * * *",
    "dbt_weekly": "0 10 * * 1",
    "embedding_reduction_item": "0 12 * * 0",
    "historize_applicative_database": "15 3 * * *",
    "import_applicative_database": "0 1 * * *",
    "import_intraday_firebase_data": "0 1 * * *",
    "link_items": {"prod": "0 20 * * 3", "stg": "0 6 * * 3", "dev": "0 6 * * 3"},
    "sync_bq_to_cloudsql_recommendation_tables": "00 6 * * *",
}


ENCRYPTED_EXPORT_DICT = {
    "20c702271b0187d039734307766eead8": {"dev": "0 14 * * 2"},
}
