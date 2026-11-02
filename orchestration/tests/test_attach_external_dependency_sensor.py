from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from airflow.operators.empty import EmptyOperator
from dags.common.dbt.dag_utils import attach_external_dependency_sensor

UTC = timezone.utc


class TestAttachExternalDependencySensor:
    """Test meta-driven configuration of the external-dependency sensor.

    external_dag_id/external_task_id stay top-level (they gate whether a
    sensor is attached at all); everything else -- soft_fail, offset_days/
    offset_hours, and arbitrary sensor kwargs like poke_interval/timeout --
    lives under meta.external_dependency_extra_params and is forwarded
    verbatim, by its real parameter name, with no translation layer.
    """

    def _base_meta(self):
        return {
            "external_dag_id": "item_embedding",
            "external_task_id": "all_items_offer_names.load_all_items_offer_names",
        }

    def test_uses_sensor_defaults_when_extra_params_is_absent(self, test_dag):
        operator = EmptyOperator(task_id="model_op", dag=test_dag)

        attach_external_dependency_sensor(
            test_dag,
            operator,
            node_tags=["external_dependency"],
            node_meta=self._base_meta(),
        )

        sensor = operator.external_dependency_sensor
        assert sensor.poke_interval == 60  # BaseSensorOperator default
        assert sensor.timeout == 604800  # Airflow's sensors.default_timeout (7 days)
        assert sensor.soft_fail is False  # BaseSensorOperator default

    def test_overrides_poke_interval_and_timeout_from_extra_params(self, test_dag):
        operator = EmptyOperator(task_id="model_op", dag=test_dag)

        attach_external_dependency_sensor(
            test_dag,
            operator,
            node_tags=["external_dependency"],
            node_meta={
                **self._base_meta(),
                "external_dependency_extra_params": {
                    "poke_interval": 600,
                    "timeout": 3600,
                },
            },
        )

        sensor = operator.external_dependency_sensor
        assert sensor.poke_interval == 600
        assert sensor.timeout == 3600

    def test_soft_fail_true_from_extra_params(self, test_dag):
        operator = EmptyOperator(task_id="model_op", dag=test_dag)

        attach_external_dependency_sensor(
            test_dag,
            operator,
            node_tags=["external_dependency"],
            node_meta={
                **self._base_meta(),
                "external_dependency_extra_params": {"soft_fail": True},
            },
        )

        assert operator.external_dependency_sensor.soft_fail is True

    @patch("common.utils.get_last_execution_date")
    def test_offset_days_and_offset_hours_from_extra_params_shift_the_window(
        self, mock_get_last_execution_date, test_dag
    ):
        operator = EmptyOperator(task_id="model_op", dag=test_dag)

        attach_external_dependency_sensor(
            test_dag,
            operator,
            node_tags=["external_dependency"],
            node_meta={
                **self._base_meta(),
                "external_dependency_extra_params": {
                    "offset_days": 1,
                    "offset_hours": -6,
                },
            },
        )

        sensor = operator.external_dependency_sensor
        logical_date = datetime(2025, 1, 2, 2, 45, tzinfo=UTC)
        sensor.execution_date_fn(logical_date)

        call_args = mock_get_last_execution_date.call_args[1]
        expected_upper = logical_date - timedelta(days=1, hours=-6)
        expected_lower = expected_upper - timedelta(days=1)
        assert call_args["upper_date_limit"] == expected_upper
        assert call_args["lower_date_limit"] == expected_lower

    def test_colliding_extra_param_raises(self, test_dag):
        operator = EmptyOperator(task_id="model_op", dag=test_dag)

        with pytest.raises(TypeError):
            attach_external_dependency_sensor(
                test_dag,
                operator,
                node_tags=["external_dependency"],
                node_meta={
                    **self._base_meta(),
                    # "trigger_rule" is already set explicitly below -- trying
                    # to override it via the passthrough dict must fail
                    # loudly instead of silently winning or losing.
                    "external_dependency_extra_params": {
                        "trigger_rule": "all_success"
                    },
                },
            )

    def test_no_sensor_attached_without_external_dependency_meta(self, test_dag):
        operator = EmptyOperator(task_id="model_op", dag=test_dag)

        attach_external_dependency_sensor(
            test_dag, operator, node_tags=[], node_meta={}
        )

        assert not hasattr(operator, "external_dependency_sensor")
