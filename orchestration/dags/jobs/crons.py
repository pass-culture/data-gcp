SCHEDULE_DICT = {
    "algo_training_two_towers": {
        "prod": "0 12 * * 4",
        "dev": "0 12 * * 2",
        "stg": "0 12 * * 3",
    },
    "clickhouse_exports": {
        "daily": {
            "prod": "45 3 * * *",
            "stg": "45 3 * * *",
            "dev": "45 3 * * *",
        },
    },
    "dbt_artifacts": "45 4 * * *",
    "dbt_monthly": "30 4 1 * *",
    "dbt_run_dag": "45 2 * * *",
    "dbt_weekly": "0 3 * * 1",
    "historize_applicative_database": "15 3 * * *",
    "import_analytics_v7": "45 4 * * *",
    "import_applicative_database": "0 1 * * *",
    "import_intraday_firebase_data": "0 1 * * *",
    "link_items": {"prod": "0 4 * * 3", "stg": "0 6 * * 3", "dev": "0 6 * * 3"},
    "retrieval_vector_build": {
        "prod": "0 6 * * *",
        "dev": "0 6 * * *",
        "stg": "0 6 * * 3",
    },
}
