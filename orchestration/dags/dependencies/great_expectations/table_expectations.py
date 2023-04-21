from dependencies.great_expectations.great_expectations_context import (
    GreatExpectationsContext,
)
import pandas as pd
from google.cloud import bigquery
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, BIGQUERY_ANALYTICS_DATASET
import os

from dependencies.great_expectations.config_historical import (
    historical_applicative_test_config,
)
from dependencies.great_expectations.utils import (
    get_table_volume_bounds,
    get_date_fields,
)

ge_root_dir = "dags/great_expectations/"


def check_directory(ge_root_dir):
    return os.listdir(ge_root_dir)


def run_applicative_history_tests():
    context = GreatExpectationsContext(ENV_SHORT_NAME, ge_root_dir)

    # Create datasources if don't exist
    if "bigquery_raw" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="raw")

    if "bigquery_clean" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="clean")

    if "bigquery_analytics" not in context.ge_context.datasources.keys():
        context.create_datasource(zone="analytics")

    # 1 - Config for tests on applicative historical tables -- Compute bounds
    for table, config in historical_applicative_test_config.items():
        volume_config_bounds = get_table_volume_bounds(
            partition_field=config["partition_field"],
            dataset_name=config["dataset_name"],
            table_name=table,
            nb_days=7,
        )
        config_name = "table_volume_bounds"
        historical_applicative_test_config[table][config_name] = volume_config_bounds

    # set up expectations + checkpoint
    for table, config in historical_applicative_test_config.items():
        context.create_expectation_suite(
            expectation_suite_name=f"volume_expectation_for_{table}",
            expectation_type="expect_table_row_count_to_be_between",
            **{
                "min_value ": config["table_volume_bounds"][0],
                "max_value": config["table_volume_bounds"][1],
            },
        )

        context.create_batch_partition(
            datasource_name="bigquery_clean",
            data_connector_name="default_runtime_data_connector_name",
            dataset_name=f"{config['dataset_name']}",
            data_asset_name=f"{table}",
            expectation_suite_name=f"volume_expectation_for_{table}",
        )

        context.create_checkpoint(
            data_asset_name=f"{config['dataset_name']}.{table}",
            checkpoint_name=f"volume_checkpoint_for_{table}",
            datasource_name="bigquery_clean",
            expectation_suite_name=f"volume_expectation_for_{table}",
        )
