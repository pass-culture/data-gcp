SCHEDULE_DICT = {
    "algo_training_two_towers": {
        "prod": "0 12 * * 4",  # every Thursday at 12:00 PM
        "dev": "0 12 * * 2",  # every Tuesday at 12:00 PM
        "stg": "0 12 * * 3",  # every Wednesday at 12:00 PM
    },
    "algo_default_deployment": "0 9 * * *",
    "artist_linkage": "0 12 * * 0",
    "bigquery_archive_partition": "0 7 * * *",
    "bigquery_historize_applicative_database": {
        "prod": "15 3 * * *",  # every day at 3:15 AM
        "stg": None,
        "dev": None,
    },
    "export_clickhouse_daily": {
        "prod": "0 6 * * *",  # every day at 6:00 AM
        "stg": "45 4 * * *",  # every day at 4:45 AM
        "dev": "45 4 * * *",  # every day at 4:45 AM
    },
    "dbt_artifacts": "0 6 * * *",
    "dbt_monthly": "30 10 1 * *",
    "dbt_run_dag": "45 2 * * *",
    "dbt_run_dag_v2": "45 2 * * *",
    "dbt_weekly": "0 10 * * 1",
    "embeddings_extraction_item": "0 12,18,23 * * *",
    "embedding_reduction_item": "0 12 * * 0",
    "import_applicative_database": "0 1 * * *",
    "import_intraday_firebase_data": "0 1 * * *",
    "link_items": {
        "prod": "0 20 * * 3",  # every Wednesday at 8:00 PM
        "stg": "0 6 * * 3",  # every Wednesday at 6:00 AM
        "dev": "0 6 * * 3",  # every Wednesday at 6:00 AM
    },
    "sync_bigquery_to_cloudsql_recommendation_tables": {
        "dev": "00 6 * * *",  # every day at 6:00 AM
        "stg": "00 6 * * *",
        "prod": "00 6 * * *",
    },
    "sync_cloudsql_recommendation_tables_to_bigquery": {
        "dev": "0 5 * * *",  # every day at 5:00 AM
        "stg": "0 5 * * *",  # every day at 5:00 AM
        "prod": "5 * * * *",  # every hour at 5 minutes past the hour
    },
}


ENCRYPTED_EXPORT_DICT = {
    "20c702271b0187d039734307766eead8": {
        "dev": None,
        "stg": None,
        "prod": "0 14 5 * *",  # every 5th of the month at 2:00 PM
    },
}
