import pytest
from unittest.mock import MagicMock, patch

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from dags.common.utils import delayed_waiting_operator
from datetime import datetime, timedelta, timezone

UTC = timezone.utc


class TestDelayedWaitingOperatorExecutionDateLogic:
    """Test the execution date calculation logic for delayed_waiting_operator."""

    @patch("dags.common.utils.get_last_execution_date")
    def test_execution_date_fn_calls_get_last_execution_date(
        self, mock_get_last_execution_date, test_dag
    ):
        """
        Test that the compute_execution_date_fn correctly calculates the window
        and calls get_last_execution_date with the right lower and upper bounds.
        """
        # Mocked return value
        expected_date = datetime(2025, 1, 1, 2, 0, tzinfo=UTC)
        mock_get_last_execution_date.return_value = expected_date

        # Create operator
        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="external_dag_id",
            external_task_id="task_1",
            offset_days=0,
            window_days=1,
            offset_hours=0,
            window_hours=0,
            skip_manually_triggered=False,
        )

        assert isinstance(operator, ExternalTaskSensor)

        # Prepare a logical_date for testing
        logical_date = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)

        # Call the execution_date_fn manually
        result = operator.execution_date_fn(logical_date)

        # Verify it returns the mocked date
        assert result == expected_date

        # Verify get_last_execution_date was called with correct bounds
        mock_get_last_execution_date.assert_called_once()
        call_args = mock_get_last_execution_date.call_args[1]  # kwargs
        expected_upper = logical_date
        expected_lower = logical_date - timedelta(days=1)
        assert call_args["upper_date_limit"] == expected_upper
        assert call_args["lower_date_limit"] == expected_lower

    @patch("dags.common.utils.get_last_execution_date")
    def test_skips_manual_dag_run(self, mock_get_last_execution_date, test_dag):
        """
        Test that a manual DAG run triggers skipping behavior (returns None)
        and does not call get_last_execution_date.
        """
        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="external_dag_id",
            skip_manually_triggered=True,
        )

        # Should be a PythonOperator
        assert isinstance(operator, PythonOperator)

        # Create a mock context representing a manual run
        context = {"dag_run": MagicMock(run_id="manual__123")}
        result = operator.python_callable(**context)

        # Manual DAG run returns None
        assert result is None

        # get_last_execution_date should never be called
        mock_get_last_execution_date.assert_not_called()

    @patch("dags.common.utils.get_last_execution_date")
    def test_waits_for_scheduled_dag_run_when_manual_skipped(
        self, mock_get_last_execution_date, test_dag
    ):
        """
        Test that the PythonOperator actually waits for scheduled DAGs
        by calling the dynamically created ExternalTaskSensor's poke.
        """
        mock_get_last_execution_date.return_value = datetime(
            2025, 1, 1, 0, 0, tzinfo=UTC
        )

        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="external_dag_id",
            skip_manually_triggered=True,
        )

        # Prepare a mock context representing a scheduled run
        context = {"dag_run": MagicMock(run_id="scheduled__20250102")}
        # Patch ExternalTaskSensor.poke to return True
        with patch.object(ExternalTaskSensor, "poke", return_value=True) as mock_poke:
            result = operator.python_callable(**context)

        # Poke should have been called once
        mock_poke.assert_called_once()
        # Result should be True from poke
        assert result is True

    @patch("dags.common.utils.get_last_execution_date")
    def test_same_daily_schedule_returns_dag_execution_date(
        self, mock_get_last_execution_date, test_dag
    ):
        """
        Test that when both DAGs are scheduled daily at the same time,
        the operator returns the execution date of the awaited DAG.
        """
        # Both DAGs scheduled daily at 2 AM
        expected_execution_date = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)
        mock_get_last_execution_date.return_value = expected_execution_date

        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="daily_dag_2am",
            external_task_id="task_1",
            offset_days=0,
            window_days=1,
            offset_hours=0,
            window_hours=0,
            skip_manually_triggered=False,
        )

        # DAG2 runs at same time as DAG1
        logical_date = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)
        result = operator.execution_date_fn(logical_date)

        assert result == expected_execution_date
        mock_get_last_execution_date.assert_called_once()

    @patch("dags.common.utils.get_last_execution_date")
    def test_dag2_earlier_than_dag1_returns_previous_dag1_run(
        self, mock_get_last_execution_date, test_dag
    ):
        """
        Test that when DAG1 is scheduled at 4am and DAG2 at 2am,
        DAG2's operator returns the previous DAG1 execution at 4am.
        """
        # DAG1 runs daily at 4am, DAG2 runs daily at 2am
        # When DAG2 runs at 2am on Jan 2, it should find DAG1's run from Jan 1 at 4am
        previous_dag1_execution = datetime(2025, 1, 1, 4, 0, tzinfo=UTC)
        mock_get_last_execution_date.return_value = previous_dag1_execution

        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="daily_dag_4am",
            external_task_id="task_1",
            offset_days=0,
            window_days=1,
            offset_hours=0,
            window_hours=0,
            skip_manually_triggered=False,
        )

        # DAG2 logical date (running at 2am)
        logical_date = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)
        result = operator.execution_date_fn(logical_date)

        assert result == previous_dag1_execution

        # Verify the window calculation includes the previous day
        call_args = mock_get_last_execution_date.call_args[1]
        expected_upper = logical_date
        expected_lower = logical_date - timedelta(days=1)
        assert call_args["upper_date_limit"] == expected_upper
        assert call_args["lower_date_limit"] == expected_lower

    @patch("dags.common.utils.get_last_execution_date")
    def test_monthly_vs_daily_first_of_month_finds_execution(
        self, mock_get_last_execution_date, test_dag
    ):
        """
        Test that when DAG1 is monthly (2am) and DAG2 is daily (4am),
        on the 1st of the month DAG2 finds DAG1's execution.
        """
        # DAG1 monthly execution on 1st at 2am
        monthly_execution_date = datetime(2025, 1, 1, 2, 0, tzinfo=UTC)
        mock_get_last_execution_date.return_value = monthly_execution_date
        window_days = 1
        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="monthly_dag_2am",
            external_task_id="task_1",
            offset_days=0,
            window_days=window_days,
            offset_hours=0,
            window_hours=0,
            skip_manually_triggered=False,
        )

        # DAG2 runs on 1st at 4am (after monthly DAG1 at 2am)
        logical_date = datetime(2025, 1, 1, 4, 0, tzinfo=UTC)
        result = operator.execution_date_fn(logical_date)

        assert result == monthly_execution_date

        # Verify the window is wide enough to catch monthly runs
        call_args = mock_get_last_execution_date.call_args[1]
        expected_upper = logical_date
        expected_lower = logical_date - timedelta(days=window_days)
        assert call_args["upper_date_limit"] == expected_upper
        assert call_args["lower_date_limit"] == expected_lower

    @patch("dags.common.utils.get_last_execution_date")
    def test_monthly_vs_daily_second_of_month_returns_none(
        self, mock_get_last_execution_date, test_dag
    ):
        """
        Test that when DAG1 is monthly (2am) and DAG2 is daily (4am),
        on the 2nd of the month DAG2 doesn't find a recent DAG1 execution.
        """
        # No recent execution found (monthly DAG1 last ran on 1st)
        mock_get_last_execution_date.return_value = None
        window_days = 1
        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="monthly_dag_2am",
            external_task_id="task_1",
            offset_days=0,
            window_days=window_days,
            offset_hours=0,
            window_hours=0,
            skip_manually_triggered=False,
        )

        # DAG2 runs on 2th at 4am
        logical_date = datetime(2025, 1, 2, 4, 0, tzinfo=UTC)
        result = operator.execution_date_fn(logical_date)

        assert result is None

        # Verify the window doesn't include the 1st of the month
        call_args = mock_get_last_execution_date.call_args[1]
        expected_upper = logical_date
        expected_lower = logical_date - timedelta(
            days=window_days
        )  # Jan 1st is outside this window
        assert call_args["upper_date_limit"] == expected_upper
        assert call_args["lower_date_limit"] == expected_lower

    @patch("dags.common.utils.get_last_execution_date")
    def test_monthly_vs_daily_with_proper_window_configuration(
        self, mock_get_last_execution_date, test_dag
    ):
        """
        Test a more realistic scenario where the window is configured appropriately
        for monthly dependencies - should find previous execution on 1st when window is large enough.
        """
        # Monthly DAG1 execution on 1st at 5am
        monthly_execution_date = datetime(2025, 1, 1, 5, 0, tzinfo=UTC)
        mock_get_last_execution_date.return_value = monthly_execution_date
        window_days = 31  # Window large enough to always catch previous monthly run
        operator = delayed_waiting_operator(
            dag=test_dag,
            external_dag_id="monthly_dag_2am",
            external_task_id="task_1",
            offset_days=0,
            window_days=window_days,
            offset_hours=0,
            window_hours=0,
            skip_manually_triggered=False,
        )

        # DAG2 runs on 31th at 4am - should find the previous monthly execution
        logical_date = datetime(2025, 1, 31, 4, 0, tzinfo=UTC)
        result = operator.execution_date_fn(logical_date)

        assert result == monthly_execution_date

        call_args = mock_get_last_execution_date.call_args[1]
        expected_upper = logical_date
        expected_lower = logical_date - timedelta(days=window_days)
        assert call_args["upper_date_limit"] == expected_upper
        assert call_args["lower_date_limit"] == expected_lower


if __name__ == "__main__":
    pytest.main()
