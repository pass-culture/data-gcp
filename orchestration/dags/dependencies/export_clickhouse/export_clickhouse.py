from common.config import ENV_SHORT_NAME


def generate_table_configs(models):
    """
    Generates configuration for Clickhous export table from DBT models to GCS
    DBT models are prefixed with 'exp_clickhouse__{model_name}' but will be aliased in BigQuery 'exp_{env}.clickhouse_{model_name}'.

    Returns:
        list: List of table configurations.
    """

    return [
        {
            "dbt_model": f"exp_clickhouse__{model_name}",
            "bigquery_dataset_name": f"export_{ENV_SHORT_NAME}",
            "bigquery_table_name": f"clickhouse_{model_name}",
            "clickhouse_table_name": model_name,
            "clickhouse_dataset_name": "intermediate",
            "mode": mode,
            "partition_key": partition_key,
        }
        for model_name, mode, partition_key in models
    ]


def generate_views_configs(table_names):
    """
    Generates configuration for views in ClickHouse.

    Returns:
        list: List of view configurations.
    """
    return [
        {
            "clickhouse_dataset_name": dataset_name,
            "clickhouse_table_name": table_name,
        }
        for dataset_name, table_name in table_names
    ]


# List of models to be exported
DBT_MODELS = [
    ("booking", "overwrite", "update_date"),
    ("collective_booking", "overwrite", "update_date"),
    ("native_event", "incremental", "partition_date"),
]
# List of aggreated tables names to be refreshed
CLICKHOUSE_TABLES = [
    ("analytics", "daily_aggregated_offer_event"),
    ("analytics", "monthly_aggregated_offerer_collective_revenue"),
    ("analytics", "monthly_aggregated_offerer_revenue"),
    ("analytics", "yearly_aggregated_offerer_collective_revenue"),
    ("analytics", "yearly_aggregated_offerer_revenue"),
]

TABLES_CONFIGS = generate_table_configs(models=DBT_MODELS)
VIEWS_CONFIGS = generate_views_configs(table_names=CLICKHOUSE_TABLES)
