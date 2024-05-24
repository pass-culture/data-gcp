TABLES_CONFIGS = [
    {
        "sql": "export_user_consultation",
        "clickhouse_table_name": "user_consultation",
        "clickhouse_dataset_name": "intermediate",
        "mode": "incremental",
    },
    {
        "sql": "export_offer_consultation",
        "clickhouse_table_name": "offer_consultation",
        "clickhouse_dataset_name": "intermediate",
        "mode": "incremental",
    },
    {
        "sql": "export_venue_offer_statistics",
        "clickhouse_table_name": "venue_offer_statistics",
        "clickhouse_dataset_name": "analytics",
        "mode": "overwrite",
    },
]

TABLES_CONFIGS2 = [
    {
        "sql": "clickhouse__booking",
        "clickhouse_table_name": "booking",
        "clickhouse_dataset_name": "intermediate",
        "mode": "overwrite",
    },
    {
        "sql": "clickhouse__collective_booking",
        "clickhouse_table_name": "collective_booking",
        "clickhouse_dataset_name": "intermediate",
        "mode": "overwrite",
    },
    {
        "sql": "clickhouse__native_event",
        "clickhouse_table_name": "native_event",
        "clickhouse_dataset_name": "intermediate",
        "mode": "incremental",
    },
]
