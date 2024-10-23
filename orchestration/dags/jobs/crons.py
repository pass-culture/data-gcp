schedule_dict = {
    "import_applicative_database": "0 1 * * *",
    "import_intraday_firebase_data": "0 1 * * *",
    "dbt_run_dag": "45 2 * * *",
    "clickhouse_exports": {
        "daily": {
            "prod": "45 3 * * *",
            "stg": "45 3 * * *",
            "dev": "45 3 * * *",
        },
    },
    "import_analytics_v7": "45 4 * * *",
    "dbt_artifacts": "45 4 * * *",
    "dbt_weekly": "0 3 * * 1",
    "dbt_monthly": "30 5 * * *",
    "historize_applicative_database": "15 3 * * *",
}
