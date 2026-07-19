"""Tests for ProphetModel — ÉLEVÉ gaps:
  - _filter_changepoints
  - evaluate()
  - run_backtest()
  - aggregate_to_monthly()

Config loading and BigQuery are mocked; Prophet model is stubbed with MagicMock.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from forecast.forecasters.forecast_model import DataSplit
from forecast.forecasters.prophet_model import ProphetModel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_CONFIG = {
    "prophet": {
        "growth": "linear",
        "changepoints": [],
        "changepoint_prior_scale": 0.05,
        "yearly_seasonality": False,
        "weekly_seasonality": False,
        "daily_seasonality": False,
        "seasonality_mode": "additive",
        "seasonality_prior_scale": 1.0,
        "interval_width": 0.8,
        "scaling": "minmax",
    },
    "features": {
        "adding_country_holidays": False,
        "add_monthly_seasonality": False,
        "monthly_fourier_order": 3,
        "pass_culture_months": None,
        "regressors": None,
    },
    "data_processing": {
        "train_prop": 0.8,
        "table_name": "test_table",
        "date_column_name": "ds",
        "target_name": "y",
    },
    "evaluation": {
        "cv": False,
        "cv_initial": 0.7,
        "cv_period": 0.15,
        "cv_horizon": 0.1,
        "freq": "D",
    },
}


def _make_model(prophet_overrides: dict | None = None, eval_overrides: dict | None = None) -> ProphetModel:
    """Instantiate a ProphetModel with the YAML config fully mocked."""
    config = {
        **_BASE_CONFIG,
        "prophet": {**_BASE_CONFIG["prophet"], **(prophet_overrides or {})},
        "evaluation": {**_BASE_CONFIG["evaluation"], **(eval_overrides or {})},
    }
    with (
        patch("forecast.forecasters.prophet_model.yaml.safe_load", return_value=config),
        patch("forecast.forecasters.prophet_model.open"),
        patch("forecast.forecasters.prophet_model.Path.exists", return_value=True),
    ):
        return ProphetModel(model_name="test_model")


def _make_split(n_days: int = 31, y: float = 100.0) -> DataSplit:
    dates = pd.date_range("2023-01-01", periods=n_days, freq="D")
    df = pd.DataFrame({"ds": dates, "y": [y] * n_days})
    return DataSplit(train=df, test=df.copy(), backtest=df.copy())


# ---------------------------------------------------------------------------
# _filter_changepoints  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestFilterChangepoints:
    @staticmethod
    def test_removes_changepoints_before_train_start():
        model = _make_model(prophet_overrides={"changepoints": [date(2019, 6, 1)]})
        model._filter_changepoints("2020-01-01", "2021-01-01")
        assert model.config.prophet.changepoints == []

    @staticmethod
    def test_removes_changepoints_after_or_equal_to_backtest_start():
        model = _make_model(prophet_overrides={"changepoints": [date(2021, 1, 1), date(2022, 5, 1)]})
        model._filter_changepoints("2020-01-01", "2021-01-01")
        assert model.config.prophet.changepoints == []

    @staticmethod
    def test_keeps_changepoints_strictly_within_training_range():
        model = _make_model(
            prophet_overrides={"changepoints": [date(2020, 3, 1), date(2020, 6, 1), date(2020, 12, 31)]}
        )
        model._filter_changepoints("2020-01-01", "2021-01-01")
        assert len(model.config.prophet.changepoints) == 3

    @staticmethod
    def test_keeps_only_in_range_when_mixed():
        model = _make_model(
            prophet_overrides={
                "changepoints": [
                    date(2019, 6, 1),  # before train_start → dropped
                    date(2020, 6, 1),  # within range → kept
                    date(2021, 1, 1),  # on backtest_start boundary → dropped (strict <)
                ]
            }
        )
        model._filter_changepoints("2020-01-01", "2021-01-01")
        assert model.config.prophet.changepoints == [date(2020, 6, 1)]

    @staticmethod
    def test_empty_changepoints_list_is_a_noop():
        model = _make_model()  # changepoints=[]
        model._filter_changepoints("2020-01-01", "2021-01-01")
        assert model.config.prophet.changepoints == []


# ---------------------------------------------------------------------------
# aggregate_to_monthly  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestAggregateToMonthly:
    @staticmethod
    def test_aggregates_daily_yhat_to_total_pricing():
        model = _make_model()
        # Jan 2023: 31 days, yhat=1.0 → total_pricing = 31.0
        dates = pd.date_range("2023-01-01", periods=31, freq="D")
        result = model.aggregate_to_monthly(pd.DataFrame({"ds": dates, "yhat": [1.0] * 31}))
        assert "total_pricing" in result.columns
        assert "ds" in result.columns
        assert result["total_pricing"].iloc[0] == pytest.approx(31.0)

    @staticmethod
    def test_produces_one_row_per_calendar_month():
        model = _make_model()
        # Jan 2023 (31 days) + Mar 2023 (31 days) — Feb avoided (28 days < 30 threshold)
        jan = pd.date_range("2023-01-01", "2023-01-31", freq="D")
        mar = pd.date_range("2023-03-01", "2023-03-31", freq="D")
        dates = list(jan) + list(mar)
        result = model.aggregate_to_monthly(pd.DataFrame({"ds": dates, "yhat": [1.0] * len(dates)}))
        assert len(result) == 2

    @staticmethod
    def test_daily_model_drops_last_month_with_fewer_than_30_rows():
        model = _make_model(eval_overrides={"freq": "D"})
        # Full Jan (31 days) + only 5 days of Feb → Feb should be dropped
        jan = pd.date_range("2023-01-01", periods=31, freq="D")
        feb_partial = pd.date_range("2023-02-01", periods=5, freq="D")
        dates = list(jan) + list(feb_partial)
        result = model.aggregate_to_monthly(pd.DataFrame({"ds": dates, "yhat": [1.0] * len(dates)}))
        assert len(result) == 1
        assert pd.Timestamp(result["ds"].iloc[0]).month == 1

    @staticmethod
    def test_daily_model_keeps_last_month_with_30_or_more_rows():
        model = _make_model(eval_overrides={"freq": "D"})
        # Jan 31 days (kept) + Mar 31 days (kept)
        jan = pd.date_range("2023-01-01", "2023-01-31", freq="D")
        mar = pd.date_range("2023-03-01", "2023-03-31", freq="D")
        dates = list(jan) + list(mar)
        result = model.aggregate_to_monthly(pd.DataFrame({"ds": dates, "yhat": [1.0] * len(dates)}))
        assert len(result) == 2

    @staticmethod
    def test_weekly_model_drops_last_month_with_fewer_than_4_rows():
        model = _make_model(eval_overrides={"freq": "W"})
        # 5 Mondays in Jan 2023 (kept: ≥4) + 1 Monday in Feb (dropped: <4)
        jan_weeks = pd.date_range("2023-01-02", periods=5, freq="W-MON")
        feb_weeks = pd.date_range("2023-02-06", periods=1, freq="W-MON")
        dates = list(jan_weeks) + list(feb_weeks)
        result = model.aggregate_to_monthly(pd.DataFrame({"ds": dates, "yhat": [1.0] * len(dates)}))
        assert len(result) == 1

    @staticmethod
    def test_weekly_model_keeps_last_month_with_4_or_more_rows():
        model = _make_model(eval_overrides={"freq": "W"})
        # Jan 2023: 5 Mondays; Feb 2023: 4 Mondays (6, 13, 20, 27)
        jan_weeks = pd.date_range("2023-01-02", periods=5, freq="W-MON")
        feb_weeks = pd.date_range("2023-02-06", periods=4, freq="W-MON")
        dates = list(jan_weeks) + list(feb_weeks)
        result = model.aggregate_to_monthly(pd.DataFrame({"ds": dates, "yhat": [1.0] * len(dates)}))
        assert len(result) == 2


# ---------------------------------------------------------------------------
# evaluate()  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestProphetModelEvaluate:
    @staticmethod
    def test_delegates_to_evaluation_pipeline_and_returns_metrics():
        model = _make_model()
        model.model = MagicMock()
        model.data_split = _make_split()

        expected = {"MAE": 10.0, "RMSE": 12.0, "MAPE": 0.05}
        with patch(
            "forecast.forecasters.prophet_model.evaluation_pipeline",
            return_value=expected,
        ) as mock_pipeline:
            metrics = model.evaluate()

        mock_pipeline.assert_called_once_with(
            model=model.model,
            df_test=model.data_split.test,
            model_config=model.config,
        )
        assert metrics == expected

    @staticmethod
    def test_raises_attribute_error_when_model_not_trained():
        model = _make_model()
        model.data_split = _make_split()
        # model.model is None → evaluation_pipeline will receive None as `model`
        with (
            patch(
                "forecast.forecasters.prophet_model.evaluation_pipeline",
                side_effect=AttributeError,
            ),
            pytest.raises(AttributeError),
        ):
            model.evaluate()


# ---------------------------------------------------------------------------
# run_backtest()  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestProphetModelRunBacktest:
    @staticmethod
    def test_delegates_to_backtest_pipeline_with_correct_args():
        model = _make_model()
        model.model = MagicMock()
        model.data_split = _make_split()

        backtest_df = pd.DataFrame({"ds": pd.date_range("2023-01-01", periods=5), "yhat": [1.0] * 5})
        expected_metrics = {"MAE": 5.0, "RMSE": 7.0, "MAPE": 0.02}

        with patch(
            "forecast.forecasters.prophet_model.backtest_pipeline",
            return_value=(expected_metrics, backtest_df),
        ) as mock_pipeline:
            metrics, forecast = model.run_backtest()

        mock_pipeline.assert_called_once_with(
            df_backtest=model.data_split.backtest,
            model=model.model,
        )
        assert metrics == expected_metrics
        assert not forecast.empty

    @staticmethod
    def test_returns_tuple_of_dict_and_dataframe():
        model = _make_model()
        model.model = MagicMock()
        model.data_split = _make_split()

        stub_df = pd.DataFrame({"ds": [pd.Timestamp("2023-01-01")], "yhat": [1.0]})
        with patch(
            "forecast.forecasters.prophet_model.backtest_pipeline",
            return_value=({"MAE": 0.0, "RMSE": 0.0, "MAPE": 0.0}, stub_df),
        ):
            result = model.run_backtest()

        assert isinstance(result, tuple)
        assert isinstance(result[0], dict)
        assert isinstance(result[1], pd.DataFrame)
