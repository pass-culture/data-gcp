# Adjust import path if necessary, assuming run from jobs/ml_jobs/finance root
# or that the workspace puts it in path.
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from forecast.forecasters.prophet_model import ProphetModel


class TestProphetModel(unittest.TestCase):
    def setUp(self):
        # Create a dummy dataframe for testing
        dates = pd.date_range(start="2020-01-01", end="2024-01-01", freq="D")
        self.dummy_df = pd.DataFrame(
            {
                "ds": dates,
                "y": np.random.default_rng(42).random(len(dates)) + 10,
            }
        )

    @patch("forecast.forecasters.prophet_model.yaml.safe_load")
    @patch("forecast.forecasters.prophet_model.open")
    @patch("forecast.forecasters.prophet_model.Path.exists")
    @patch("forecast.engines.prophet.preprocessing.load_table")
    def test_happy_path(self, mock_load_table, mock_exists, mock_open, mock_yaml):
        """test_happy_path: Check if model initializes, prepares data, trains,
        and predicts."""

        # 1. Setup Data Mock
        mock_load_table.return_value = self.dummy_df

        # 2. Setup Config Mock
        mock_exists.return_value = True  # Config file exists
        mock_config = {
            "prophet": {
                "growth": "linear",
                "changepoints": [],
                "changepoint_prior_scale": 0.05,
                "changepoint_range": 0.9,
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
                "pass_culture_seasonality": False,
                "regressors": None,
                "regressor_future_strategy": "hold_last_value",
            },
            "data_processing": {
                "train_prop": 0.8,
                "table_name": "test_table",
                "date_column_name": "ds",
                "target_name": "y",
            },
            "evaluation": {
                "cv": False,
                "cv_initial": "0.7",
                "cv_period": "0.15",
                "cv_horizon": "0.1",
                "freq": "D",
            },
        }
        mock_yaml.return_value = mock_config

        # 3. Initialize Model
        # We dummy the model_name. The mock_load_config handles the file read.
        model = ProphetModel(model_name="test_model")

        assert model.config is not None
        assert model.config.data_processing.table_name == "test_table"

        # 4. Prepare Data
        # Using date strings within the dummy_df range
        model.prepare_data(
            dataset="ml_finance_test",
            train_start_date="2020-01-01",
            backtest_start_date="2023-01-01",
            backtest_end_date="2023-06-01",
        )

        assert model.data_split is not None
        assert not model.data_split.train.empty

        # 5. Train
        _ = model.train()
        assert model.model is not None
        # Check simple property of trained model
        assert hasattr(model.model, "params")

        # 6. Predict
        # Forecast 30 days ahead
        forecast_df = model.predict(start_date="2023-06-01", end_date="2023-07-01")
        assert not forecast_df.empty
        assert "yhat" in forecast_df.columns
        assert "ds" in forecast_df.columns

    @patch("forecast.forecasters.prophet_model.yaml.safe_load")
    @patch("forecast.forecasters.prophet_model.open")
    @patch("forecast.forecasters.prophet_model.Path.exists")
    @patch("forecast.engines.prophet.preprocessing.load_table")
    def test_regressor_end_to_end(self, mock_load_table, mock_exists, mock_open, mock_yaml):
        """A configured regressor must be present in training data and get a
        future value populated automatically (regression test for
        create_full_prediction_dataframe, which previously never filled
        regressor columns on the future dataframe)."""
        dummy_df = self.dummy_df.copy()
        dummy_df["total_available_credits"] = np.random.default_rng(0).random(len(dummy_df)) * 1e6 + 5e6
        mock_load_table.return_value = dummy_df

        mock_exists.return_value = True
        mock_config = {
            "prophet": {
                "growth": "linear",
                "changepoints": [],
                "changepoint_prior_scale": 0.05,
                "changepoint_range": 0.9,
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
                "pass_culture_seasonality": False,
                "regressors": ["total_available_credits"],
                "regressor_future_strategy": "hold_last_value",
            },
            "data_processing": {
                "train_prop": 0.8,
                "table_name": "test_table",
                "date_column_name": "ds",
                "target_name": "y",
            },
            "evaluation": {
                "cv": False,
                "cv_initial": "0.7",
                "cv_period": "0.15",
                "cv_horizon": "0.1",
                "freq": "D",
            },
        }
        mock_yaml.return_value = mock_config

        model = ProphetModel(model_name="test_model")
        model.prepare_data(
            dataset="ml_finance_test",
            train_start_date="2020-01-01",
            backtest_start_date="2023-01-01",
            backtest_end_date="2023-06-01",
        )
        model.train()

        forecast_df = model.predict(start_date="2023-06-01", end_date="2023-07-01")
        assert not forecast_df.empty
        assert forecast_df["yhat"].notna().all()

    @patch("forecast.forecasters.prophet_model.yaml.safe_load")
    @patch("forecast.forecasters.prophet_model.open")
    @patch("forecast.forecasters.prophet_model.Path.exists")
    def test_aggregate_to_monthly_keeps_bands(self, mock_exists, mock_open, mock_yaml):
        """aggregate_to_monthly must carry yhat_lower/yhat_upper through as
        total_pricing_low/high, not just yhat, so the report can render
        scenario bands."""
        mock_exists.return_value = True
        mock_config = {
            "prophet": {
                "growth": "linear",
                "changepoints": [],
                "changepoint_prior_scale": 0.05,
                "changepoint_range": 0.9,
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
                "pass_culture_seasonality": False,
                "regressors": None,
                "regressor_future_strategy": "hold_last_value",
            },
            "data_processing": {
                "train_prop": 0.8,
                "table_name": "test_table",
                "date_column_name": "ds",
                "target_name": "y",
            },
            "evaluation": {
                "cv": False,
                "cv_initial": "0.7",
                "cv_period": "0.15",
                "cv_horizon": "0.1",
                "freq": "D",
            },
        }
        mock_yaml.return_value = mock_config
        model = ProphetModel(model_name="test_model")

        dates = pd.date_range("2024-01-01", periods=31, freq="D")
        forecast_df = pd.DataFrame({"ds": dates, "yhat": 10.0, "yhat_lower": 8.0, "yhat_upper": 12.0})
        monthly = model.aggregate_to_monthly(forecast_df)

        assert {"total_pricing", "total_pricing_low", "total_pricing_high"} <= set(monthly.columns)
        row = monthly.iloc[0]
        assert row["total_pricing_low"] <= row["total_pricing"] <= row["total_pricing_high"]


if __name__ == "__main__":
    unittest.main()
