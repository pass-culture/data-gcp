from typing import Dict, List, Tuple, Union

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


def generate_analytics_configs(
    table_list: List[Union[Tuple[str, str], Tuple[str, str, List[str]]]],
) -> List[Dict]:
    """
    Generates configuration for views in ClickHouse.

    Args:
        table_names (list): List of tuples containing dataset name, table name, and optionally a depends list.

    Returns:
        list: List of view configurations.
    """
    configs = []

    for table in table_list:
        # Handle case with or without dependencies
        config = {
            "clickhouse_dataset_name": table[0],
            "clickhouse_table_name": table[1],
        }

        # Add depends_list if it exists
        if len(table) == 3:
            config["depends_list"] = table[2]

        configs.append(config)

    return configs


# List of models to be exported
DBT_MODELS = [
    ("booking", "overwrite", "update_date"),
    ("collective_booking", "overwrite", "update_date"),
    ("native_event", "incremental", "partition_date"),
    ("venue_offer_statistic", "overwrite", None),
]
# List of aggreated tables names to be refreshed with compressed config: (clickhouse_dataset_name, clickhouse_table_name, optionally depends_list)
CLICKHOUSE_TABLES = [
    ("analytics", "daily_aggregated_venue_offer_consultation"),
    ("analytics", "last_30_day_venue_top_offer_consultation"),
    ("analytics", "monthly_aggregated_venue_collective_revenue"),
    ("analytics", "monthly_aggregated_venue_individual_revenue"),
    (
        "analytics",
        "monthly_aggregated_venue_revenue",
        [
            "monthly_aggregated_venue_collective_revenue",
            "monthly_aggregated_venue_individual_revenue",
        ],
    ),
    ("analytics", "yearly_aggregated_venue_collective_revenue"),
    ("analytics", "yearly_aggregated_venue_individual_revenue"),
    (
        "analytics",
        "yearly_aggregated_venue_revenue",
        [
            "yearly_aggregated_venue_collective_revenue",
            "yearly_aggregated_venue_individual_revenue",
        ],
    ),
]

TABLES_CONFIGS = generate_table_configs(models=DBT_MODELS)
ANALYTICS_CONFIGS = generate_analytics_configs(table_list=CLICKHOUSE_TABLES)
