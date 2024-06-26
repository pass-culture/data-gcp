from ruamel.yaml import YAML
from datetime import datetime

import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.exceptions import DataContextError
from great_expectations.cli.datasource import (
    sanitize_yaml_and_save_datasource,
)


class GreatExpectationsContext:
    def __init__(self, env_short_name, ge_root_dir):
        self.env_short_name = env_short_name
        self.ge_context = gx.data_context.DataContext(ge_root_dir)

    def create_datasource(self, zone):
        # zone : raw|clean|analytics

        self.datasource_name = f"bigquery_{zone}"
        connection_string = f"bigquery:///{zone}_" + "${self.env_short_name}"
        schema_name = f"{zone}" + "_${self.env_short_name}"  # or dataset name
        config_yaml = f"""
            name: {self.datasource_name}
            class_name: Datasource
            execution_engine:
            class_name: SqlAlchemyExecutionEngine
            connection_string: {connection_string}
            data_connectors:
            default_runtime_data_connector_name:
                class_name: RuntimeDataConnector
                batch_identifiers:
                - default_identifier_name
            default_inferred_data_connector_name:
                class_name: InferredAssetSqlDataConnector
                include_schema_name: True
                introspection_directives:
                schema_name: {schema_name}
        """

        self.ge_context.test_yaml_config(yaml_config=config_yaml)

        # Save the datasource config in great_expectations.yml
        sanitize_yaml_and_save_datasource(
            self.ge_context, config_yaml, overwrite_existing=False
        )

        print(self.ge_context.list_datasources())

        return self.ge_context

    def create_expectation_suite(
        self, expectation_suite_name, expectation_type, **kwargs
    ):
        try:
            suite = self.ge_context.get_expectation_suite(
                expectation_suite_name=expectation_suite_name
            )
            print(
                f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.'
            )
        except DataContextError:
            suite = self.ge_context.create_expectation_suite(
                expectation_suite_name=expectation_suite_name
            )
            print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

        # Config of the expectation
        expectation_configuration = ExpectationConfiguration(
            **{"meta": {}, "expectation_type": expectation_type, "kwargs": kwargs}
        )

        suite.add_expectation(expectation_configuration=expectation_configuration)
        # Print the config of the suite
        # print(self.ge_context.get_expectation_suite(expectation_suite_name=expectation_suite_name))
        self.ge_context.save_expectation_suite(
            expectation_suite=suite, expectation_suite_name=expectation_suite_name
        )

    def create_batch(
        self,
        datasource_name,
        data_connector_name,
        dataset_name,
        data_asset_name,
        expectation_suite_name,
        partitioned=False,
    ):
        """Create a specific batch to validate tests over last partition."""

        if partitioned is True:
            today_date = datetime.today().strftime("%Y-%m-%d")

            batch_request = RuntimeBatchRequest(
                datasource_name=datasource_name,
                data_connector_name=data_connector_name,
                data_asset_name=data_asset_name,
                runtime_parameters={
                    "query": f"SELECT * from {dataset_name}.{data_asset_name} WHERE partition_date = '{today_date}'"
                },
                batch_identifiers={"default_identifier_name": "default_identifier"},
            )

            validator = self.ge_context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
            )

            self.batch_request = batch_request
            self.validator = validator

            print(self.validator.batches)

    def create_and_run_checkpoint(
        self, data_asset_name, checkpoint_name, datasource_name, expectation_suite_name
    ):
        yaml = YAML()

        if hasattr(self, "batch_request"):
            yaml_config = f"""
            name: {checkpoint_name}
            config_version: 1
            class_name: Checkpoint
            run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
            validations:
              - batch_request:
                  {self.batch_request}
            expectation_suite_name: {expectation_suite_name}
            action_list:
              - name: store_validation_result
                action:
                  class_name: StoreValidationResultAction
              - name: store_evaluation_params
                action:
                  class_name: StoreEvaluationParametersAction
            """

        else:
            yaml_config = f"""
            name: {checkpoint_name}
            config_version: 1
            class_name: Checkpoint
            run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
            validations:
              - batch_request:
                  datasource_name: {datasource_name}
                  data_connector_name: default_inferred_data_connector_name
                  data_asset_name: {data_asset_name}
                  data_connector_query:
                    index: -1
            expectation_suite_name: {expectation_suite_name}
            action_list:
              - name: store_validation_result
                action:
                  class_name: StoreValidationResultAction
              - name: store_evaluation_params
                action:
                  class_name: StoreEvaluationParametersAction
            """

        my_checkpoint = self.ge_context.test_yaml_config(yaml_config=yaml_config)

        print(my_checkpoint)

        self.ge_context.add_or_update_checkpoint(**yaml.load(yaml_config))
        self.ge_context.run_checkpoint(checkpoint_name=checkpoint_name)
