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
        "sql": "clickhouse__bookings",
        "clickhouse_table_name": "bookings",
        "clickhouse_dataset_name": "intermediate",
        "mode": "overwrite",
    },
    {
        "sql": "clickhouse__collective_bookings",
        "clickhouse_table_name": "collective_bookings",
        "clickhouse_dataset_name": "intermediate",
        "mode": "overwrite",
    },
    {
        "sql": "clickhouse__native_events",
        "clickhouse_table_name": "native_events",
        "clickhouse_dataset_name": "intermediate",
        "mode": "incremental",
    },
]
