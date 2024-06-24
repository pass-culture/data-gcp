# table_config['sql'] values don't refer to sql files but tables/views in bigquery created by dbt models in folder export/clickhouse
# however theses dbt models are prefixed exp_clickhouse__modelName and aliased in BQ database as clickhouse_modelName by get_custom_alias dbt macro
# make sure to set table_config['sql'] values to clickhouse_modelName
TABLES_CONFIGS = [
    {
        "sql": "clickhouse_booking",
        "clickhouse_table_name": "booking",
        "clickhouse_dataset_name": "intermediate",
        "mode": "overwrite",
    },
    {
        "sql": "clickhouse_collective_booking",
        "clickhouse_table_name": "collective_booking",
        "clickhouse_dataset_name": "intermediate",
        "mode": "overwrite",
    },
    {
        "sql": "clickhouse_native_event",
        "clickhouse_table_name": "native_event",
        "clickhouse_dataset_name": "intermediate",
        "mode": "incremental",
    },
]

VIEWS_CONFIGS = [
    {
        "clickhouse_view_name": "daily_aggregated_event",
        "clickhouse_dataset_name": "analytics",
    },
    {
        "clickhouse_view_name": "monthly_aggregated_offerer_revenue",
        "clickhouse_dataset_name": "analytics",
    },
    {
        "clickhouse_view_name": "yearly_aggregated_offerer_revenue",
        "clickhouse_dataset_name": "analytics",
    },
]
