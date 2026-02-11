SCHEDULE_DICT = {
    "algo_training_two_towers": {
        "prod": "0 12 * * 4",  # every Thursday at 12:00 PM
        "dev": "0 12 * * 2",  # every Tuesday at 12:00 PM
        "stg": "0 12 * * 3",  # every Wednesday at 12:00 PM
    },
    "algo_training_graph_embeddings": {
        "prod": None,
        "stg": None,
        "dev": None,
    },
    "algo_default_deployment": "0 6 * * *",
    "artist_linkage": "0 12 * * 0",
    "bigquery_archive_partition": "0 7 * * *",
    "bigquery_snapshot_backup": {
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
    "dbt_run_dag": "45 2 * * *",
    "embeddings_extraction_item": "0 12,18,23 * * *",
    "embedding_reduction_item": "0 12 * * 0",
    "import_applicative_database": "0 1 * * *",
    "import_intraday_firebase_data": "0 1 * * *",
    "import_titelive": "0 2 * * *",  # every day at 2:00 AM
    "link_items": {
        "prod": "0 20 * * 3",  # every Wednesday at 8:00 PM
        "stg": "0 6 * * 3",  # every Wednesday at 6:00 AM
        "dev": "0 6 * * 3",  # every Wednesday at 6:00 AM
    },
    "sync_bigquery_to_cloudsql_recommendation_tables": {
        "dev": "00 2 * * *",  # every day at 2:00 AM UTC
        "stg": "00 2 * * *",
        "prod": "00 2 * * *",
    },
    "recommendation_endpoint_monitoring": "0 8 * * *",  # every day at 8:00 AM
    "export_external_reporting": {
        "prod": "45 4 1 * *",  # every month at 4:45 AM
    },
    "finance_pricing_forecast": {
        "prod": "0 6 * * 1",  # every Monday at 6:00 AM
        "stg": None,
        "dev": None,
    },
}


ENCRYPTED_EXPORT_DICT = {
    "20c702271b0187d039734307766eead8": {
        "dev": None,
        "stg": None,
        "prod": "0 14 5 * *",  # every 5th of the month at 2:00 PM
    },
}
