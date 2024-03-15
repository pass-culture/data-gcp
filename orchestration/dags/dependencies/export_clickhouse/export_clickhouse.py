TABLES_CONFIGS = [
    {
        "sql": "export_offer_consultation",
        "clickhouse_table_name": "offer_consultation",
        "clickhouse_database_name": "analytics",
        "mode": "incremental",
        "params": {"days": 5},
    },
    {
        "sql": "export_user_consultation",
        "clickhouse_table_name": "user_consultation",
        "clickhouse_database_name": "recommendation",
        "mode": "incremental",
        "params": {"days": 5},
    },
    {
        "sql": "export_venue_offer_statistics",
        "clickhouse_table_name": "venue_offer_statistics",
        "clickhouse_database_name": "analytics",
        "mode": "overwrite",
    },
]
