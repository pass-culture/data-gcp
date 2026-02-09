# Adjust import path if necessary, assuming run from jobs/ml_jobs/finance root
# or that the workspace puts it in path.
import unittest
from unittest.mock import MagicMock, patch

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
                "cv_initial": "365 days",
                "cv_period": "30 days",
                "cv_horizon": "30 days",
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

    @patch("forecast.forecasters.prophet_model.log_diagnostic_plots")
    @patch("forecast.forecasters.prophet_model.yaml.safe_load")
    @patch("forecast.forecasters.prophet_model.Path.exists")
    @patch("forecast.forecasters.prophet_model.open")
    def test_get_diagnostics(self, mock_open, mock_exists, mock_yaml, mock_plots):
        "test_get_diagnostics: Check if it calls the plot function and returns dict."
        mock_exists.return_value = True
        mock_yaml.return_value = {
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
                "cv": False,
                "cv_initial": "1 d",
                "cv_period": "1 d",
                "cv_horizon": "1 d",
                "freq": "D",
            },
        }

        model = ProphetModel("test_model")

        # Manually set mocked internals to skip data prep/training
        model.model = MagicMock()
        model.data_split = MagicMock()
        model.data_split.train = pd.DataFrame({"ds": [], "y": []})

        mock_plots.return_value = {"trend": "figure_obj"}

        diags = model.get_diagnostics()
        assert diags == {"trend": "figure_obj"}
        mock_plots.assert_called_once()


if __name__ == "__main__":
    unittest.main()
