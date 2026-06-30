"""Tests for forecast.engines.prophet.evaluate — CRITIQUE & ÉLEVÉ gaps.

Prophet model is mocked via MagicMock: only evaluate.py logic is exercised.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from forecast.engines.prophet.evaluate import (
    _aggregate_forecast_monthly,
    _convert_cv_param,
    backtest_pipeline,
    compute_metrics,
    evaluation_pipeline,
    predict_with_truth,
)
from forecast.engines.prophet.model_config import ModelConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_eval_config(cv: bool = False, freq: str = "D") -> ModelConfig:
    return ModelConfig(
        **{
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
                "table_name": "t",
                "date_column_name": "ds",
                "target_name": "y",
            },
            "evaluation": {
                "cv": cv,
                "cv_initial": 0.7,
                "cv_period": 0.15,
                "cv_horizon": 0.1,
                "freq": freq,
            },
        }
    )


def _make_prophet_mock(dates, yhat_values):
    """Return a mock Prophet model whose predict() returns a simple forecast."""
    mock_model = MagicMock()
    mock_model.predict.return_value = pd.DataFrame({"ds": pd.to_datetime(dates), "yhat": yhat_values})
    return mock_model


# ---------------------------------------------------------------------------
# _convert_cv_param  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestConvertCvParam:
    @staticmethod
    def test_daily_freq_returns_days_string():
        assert _convert_cv_param(0.5, 100, "D") == "50 days"

    @staticmethod
    def test_weekly_freq_returns_weeks_string():
        assert _convert_cv_param(0.6, 50, "W-MON") == "30 W"

    @staticmethod
    def test_weekly_freq_any_w_prefix_is_detected():
        assert _convert_cv_param(0.5, 10, "W") == "5 W"

    @staticmethod
    def test_result_is_truncated_to_integer_periods():
        # int(0.33 * 10) = int(3.3) = 3
        assert _convert_cv_param(0.33, 10, "D") == "3 days"

    @staticmethod
    def test_full_period_gives_expected_count():
        assert _convert_cv_param(1.0, 365, "D") == "365 days"


# ---------------------------------------------------------------------------
# compute_metrics  🚨 CRITIQUE
# ---------------------------------------------------------------------------


class TestComputeMetrics:
    @staticmethod
    def test_perfect_forecast_produces_zero_errors():
        forecast = pd.DataFrame({"y": [10.0, 20.0, 30.0], "yhat": [10.0, 20.0, 30.0]})
        metrics = compute_metrics(forecast)
        assert metrics["MAE"] == pytest.approx(0.0)
        assert metrics["RMSE"] == pytest.approx(0.0)
        assert metrics["MAPE"] == pytest.approx(0.0)

    @staticmethod
    def test_known_mae_is_computed_correctly():
        # |100-110| + |200-180| = 10 + 20 → MAE = 15
        forecast = pd.DataFrame({"y": [100.0, 200.0], "yhat": [110.0, 180.0]})
        metrics = compute_metrics(forecast)
        assert metrics["MAE"] == pytest.approx(15.0)

    @staticmethod
    def test_known_rmse_is_computed_correctly():
        # sqrt((10² + 20²) / 2) = sqrt(250) ≈ 15.811
        forecast = pd.DataFrame({"y": [100.0, 200.0], "yhat": [110.0, 180.0]})
        metrics = compute_metrics(forecast)
        assert metrics["RMSE"] == pytest.approx((250.0**0.5), rel=1e-4)

    @staticmethod
    def test_returns_exactly_three_keys():
        forecast = pd.DataFrame({"y": [1.0, 2.0, 3.0], "yhat": [1.1, 2.1, 2.9]})
        metrics = compute_metrics(forecast)
        assert set(metrics.keys()) == {"MAE", "RMSE", "MAPE"}

    @staticmethod
    def test_mape_computed_as_relative_error():
        # |10-11| / 10 = 0.1 → MAPE = 10%
        forecast = pd.DataFrame({"y": [10.0], "yhat": [11.0]})
        metrics = compute_metrics(forecast)
        assert metrics["MAPE"] == pytest.approx(0.1, rel=1e-4)


# ---------------------------------------------------------------------------
# predict_with_truth  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestPredictWithTruth:
    @staticmethod
    def test_output_contains_both_yhat_and_y():
        dates = pd.date_range("2023-01-01", periods=3, freq="D")
        mock_model = _make_prophet_mock(dates, [11.0, 19.0, 31.0])
        df_actual = pd.DataFrame({"ds": dates, "y": [10.0, 20.0, 30.0]})
        result = predict_with_truth(mock_model, df_actual)
        assert "yhat" in result.columns
        assert "y" in result.columns

    @staticmethod
    def test_y_values_match_actuals_after_merge():
        dates = pd.date_range("2023-01-01", periods=3, freq="D")
        mock_model = _make_prophet_mock(dates, [10.0, 20.0, 30.0])
        df_actual = pd.DataFrame({"ds": dates, "y": [10.0, 20.0, 30.0]})
        result = predict_with_truth(mock_model, df_actual)
        pd.testing.assert_series_equal(
            result["y"].reset_index(drop=True),
            pd.Series([10.0, 20.0, 30.0]),
            check_names=False,
        )

    @staticmethod
    def test_row_count_equals_actual_dataframe():
        dates = pd.date_range("2023-01-01", periods=5, freq="D")
        mock_model = _make_prophet_mock(dates, [1.0] * 5)
        df_actual = pd.DataFrame({"ds": dates, "y": [1.0] * 5})
        result = predict_with_truth(mock_model, df_actual)
        assert len(result) == 5


# ---------------------------------------------------------------------------
# _aggregate_forecast_monthly  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestAggregateForecastMonthly:
    @staticmethod
    def test_sums_daily_y_and_yhat_to_one_month():
        # Jan 2023: 31 days, y=1, yhat=2 → monthly y=31, yhat=62
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2023-01-01", periods=31, freq="D"),
                "y": [1.0] * 31,
                "yhat": [2.0] * 31,
            }
        )
        result = _aggregate_forecast_monthly(df)
        assert len(result) == 1
        assert result["y"].iloc[0] == pytest.approx(31.0)
        assert result["yhat"].iloc[0] == pytest.approx(62.0)

    @staticmethod
    def test_produces_one_row_per_calendar_month():
        # Jan + Feb 2023
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2023-01-01", periods=59, freq="D"),
                "y": [1.0] * 59,
                "yhat": [1.0] * 59,
            }
        )
        result = _aggregate_forecast_monthly(df)
        assert len(result) == 2

    @staticmethod
    def test_optional_columns_yhat_lower_upper_are_summed_when_present():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2023-01-01", periods=31, freq="D"),
                "y": [1.0] * 31,
                "yhat": [1.0] * 31,
                "yhat_lower": [0.9] * 31,
                "yhat_upper": [1.1] * 31,
            }
        )
        result = _aggregate_forecast_monthly(df)
        assert "yhat_lower" in result.columns
        assert "yhat_upper" in result.columns
        assert result["yhat_lower"].iloc[0] == pytest.approx(31 * 0.9)

    @staticmethod
    def test_absent_optional_columns_are_not_added():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2023-01-01", periods=31, freq="D"),
                "y": [1.0] * 31,
                "yhat": [1.0] * 31,
            }
        )
        result = _aggregate_forecast_monthly(df)
        assert "yhat_lower" not in result.columns
        assert "yhat_upper" not in result.columns


# ---------------------------------------------------------------------------
# evaluation_pipeline  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestEvaluationPipeline:
    @staticmethod
    def test_non_cv_path_returns_mae_rmse_mape():
        dates = pd.date_range("2023-01-01", periods=5, freq="D")
        mock_model = _make_prophet_mock(dates, [10.0] * 5)
        df_test = pd.DataFrame({"ds": dates, "y": [10.0] * 5})
        config = make_eval_config(cv=False)
        metrics = evaluation_pipeline(mock_model, df_test, config)
        assert set(metrics.keys()) == {"MAE", "RMSE", "MAPE"}

    @staticmethod
    def test_non_cv_path_with_perfect_forecast_gives_zero_errors():
        dates = pd.date_range("2023-01-01", periods=5, freq="D")
        mock_model = _make_prophet_mock(dates, [100.0] * 5)
        df_test = pd.DataFrame({"ds": dates, "y": [100.0] * 5})
        metrics = evaluation_pipeline(mock_model, df_test, make_eval_config(cv=False))
        assert metrics["MAE"] == pytest.approx(0.0)

    @staticmethod
    def test_cv_path_calls_cross_validate_and_not_model_predict():
        dates = pd.date_range("2023-01-01", periods=5, freq="D")
        mock_model = _make_prophet_mock(dates, [10.0] * 5)
        df_test = pd.DataFrame({"ds": dates, "y": [10.0] * 5})
        config = make_eval_config(cv=True)

        with patch(
            "forecast.engines.prophet.evaluate.cross_validate",
            return_value={"MAE": 5.0, "RMSE": 6.0, "MAPE": 0.05},
        ) as mock_cv:
            metrics = evaluation_pipeline(mock_model, df_test, config)
            mock_cv.assert_called_once()
            mock_model.predict.assert_not_called()

        assert metrics == {"MAE": 5.0, "RMSE": 6.0, "MAPE": 0.05}


# ---------------------------------------------------------------------------
# backtest_pipeline  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestBacktestPipeline:
    @staticmethod
    def test_returns_metrics_dict_and_forecast_dataframe():
        dates = pd.date_range("2023-01-01", periods=31, freq="D")
        mock_model = _make_prophet_mock(dates, [100.0] * 31)
        df_backtest = pd.DataFrame({"ds": dates, "y": [100.0] * 31})
        metrics, forecast_df = backtest_pipeline(df_backtest, mock_model)
        assert isinstance(metrics, dict)
        assert set(metrics.keys()) == {"MAE", "RMSE", "MAPE"}
        assert not forecast_df.empty
        assert "yhat" in forecast_df.columns

    @staticmethod
    def test_metrics_are_computed_at_monthly_level():
        """Perfect daily forecast → monthly aggregation is also perfect → MAE = 0."""
        dates = pd.date_range("2023-01-01", periods=31, freq="D")
        mock_model = _make_prophet_mock(dates, [100.0] * 31)
        df_backtest = pd.DataFrame({"ds": dates, "y": [100.0] * 31})
        metrics, _ = backtest_pipeline(df_backtest, mock_model)
        assert metrics["MAE"] == pytest.approx(0.0)

    @staticmethod
    def test_forecast_dataframe_contains_original_frequency_rows():
        """The returned forecast_df must preserve daily granularity, not monthly."""
        dates = pd.date_range("2023-01-01", periods=31, freq="D")
        mock_model = _make_prophet_mock(dates, [1.0] * 31)
        df_backtest = pd.DataFrame({"ds": dates, "y": [1.0] * 31})
        _, forecast_df = backtest_pipeline(df_backtest, mock_model)
        assert len(forecast_df) == 31
