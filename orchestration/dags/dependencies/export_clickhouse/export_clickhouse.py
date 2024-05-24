TABLES_CONFIGS = [
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
