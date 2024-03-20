TABLES_CONFIGS = [
    {
        "sql": "export_offer_consultation",
        "clickhouse_table_name": "offer_consultation",
        "clickhouse_dataset_name": "analytics",
        "mode": "incremental",
    },
    {
        "sql": "export_user_consultation",
        "clickhouse_table_name": "user_consultation",
        "clickhouse_dataset_name": "recommendation",
        "mode": "incremental",
    },
    {
        "sql": "export_venue_offer_statistics",
        "clickhouse_table_name": "venue_offer_statistics",
        "clickhouse_dataset_name": "analytics",
        "mode": "overwrite",
    },
]
