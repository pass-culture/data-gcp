schedule_dict = {
    "import_applicative_database": "0 1 * * *",
    "import_intraday_firebase_data": "0 1 * * *",
    "dbt_run_dag": "30 2 * * *",
    "dbt_artifacts": "0 5 * * *",
    "import_analytics_v7": "0 5 * * *",
    "clickhouse_exports": {
        "daily": {
            "prod": "0 5 * * *",
            "stg": "0 5 * * *",
            "dev": "0 5 * * *",
        },
    },
    "dbt_weekly": "30 5 * * *",
    "dbt_montly": "30 5 * * *",
}
