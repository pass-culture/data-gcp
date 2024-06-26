
from common.config import ENV_SHORT_NAME

from dependencies.great_expectations.great_expectations_context import (
    GreatExpectationsContext,
)
from dependencies.great_expectations.utils import get_table_volume_bounds, ge_root_dir


def run_applicative_history_tests(table_name, config):
    context = GreatExpectationsContext(ENV_SHORT_NAME, ge_root_dir)
    # Create datasources if don't exist
    if "bigquery_raw" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="raw")

    if "bigquery_clean" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="clean")

    if "bigquery_analytics" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="analytics")

    # 1 - Config for tests on applicative historical tables -- Compute bounds
    volume_config_bounds = get_table_volume_bounds(
        partition_field=config.get("partition_field", None),
        dataset_name=config.get("dataset_name", None),
        table_name=table_name,
        nb_days=7,
    )
    config_name = "table_volume_bounds"
    config[config_name] = volume_config_bounds

    # set up expectations + checkpoint

    context.create_expectation_suite(
        expectation_suite_name=f"volume_expectation_for_{table_name}",
        expectation_type="expect_table_row_count_to_be_between",
        **{
            "min_value ": config["table_volume_bounds"][0],
            "max_value": config["table_volume_bounds"][1],
        },
    )

    context.create_batch(
        datasource_name="bigquery_clean",
        data_connector_name="default_runtime_data_connector_name",
        dataset_name=f"{config['dataset_name']}",
        data_asset_name=f"{table_name}",
        expectation_suite_name=f"volume_expectation_for_{table_name}",
        partitioned=True,
    )

    context.create_and_run_checkpoint(
        data_asset_name=f"{config['dataset_name']}.{table_name}",
        checkpoint_name=f"volume_checkpoint_for_{table_name}",
        datasource_name="bigquery_clean",
        expectation_suite_name=f"volume_expectation_for_{table_name}",
    )


def run_enriched_tests(table_name, config):
    context = GreatExpectationsContext(ENV_SHORT_NAME, ge_root_dir)

    # Create datasources if don't exist
    if "bigquery_raw" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="raw")

    if "bigquery_clean" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="clean")

    if "bigquery_analytics" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="analytics")

    context.create_expectation_suite(
        expectation_suite_name=f"freshness_expectation_for_{table_name}",
        expectation_type="expect_column_max_to_be_between",
        **{
            "column": config["date_field"],
            "min_value ": config["freshness_check"][ENV_SHORT_NAME][0],
            "max_value": config["freshness_check"][ENV_SHORT_NAME][1],
        },
    )

    context.create_batch(
        datasource_name="bigquery_analytics",
        data_connector_name="default_runtime_data_connector_name",
        dataset_name=f"{config['dataset_name']}",
        data_asset_name=f"{table_name}",
        expectation_suite_name=f"freshness_expectation_for_{table_name}",
        partitioned=False,
    )

    context.create_and_run_checkpoint(
        data_asset_name=f"{config['dataset_name']}.{table_name}",
        checkpoint_name=f"freshness_checkpoint_for_{table_name}",
        datasource_name="bigquery_analytics",
        expectation_suite_name=f"freshness_expectation_for_{table_name}",
    )
