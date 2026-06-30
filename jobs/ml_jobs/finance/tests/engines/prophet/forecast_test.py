"""Tests for forecast.engines.prophet.forecast — ÉLEVÉ gaps.

Pure functions: Prophet model mocked via MagicMock.
"""

import pandas as pd

from forecast.engines.prophet.forecast import create_full_prediction_dataframe
from forecast.engines.prophet.model_config import ModelConfig
from forecast.forecasters.forecast_model import DataSplit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_config(
    growth: str = "linear",
    freq: str = "D",
    pass_culture_months=None,
) -> ModelConfig:
    return ModelConfig(
        **{
            "prophet": {
                "growth": growth,
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
                "pass_culture_months": pass_culture_months,
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
                "cv_initial": 0.7,
                "cv_period": 0.15,
                "cv_horizon": 0.1,
                "freq": freq,
            },
        }
    )


def make_split(cap: float = 120.0, floor: float = 0.0) -> DataSplit:
    """DataSplit with a single full year of daily train data."""
    train = pd.DataFrame(
        {
            "ds": pd.date_range("2020-01-01", periods=365, freq="D"),
            "y": [100.0] * 365,
            "cap": [cap] * 365,
            "floor": [floor] * 365,
        }
    )
    return DataSplit(train=train, test=pd.DataFrame(), backtest=pd.DataFrame())


# ---------------------------------------------------------------------------
# create_full_prediction_dataframe  🔴 ÉLEVÉ
# ---------------------------------------------------------------------------


class TestCreateFullPredictionDataframe:
    @staticmethod
    def test_returns_daily_date_range_with_ds_column():
        result = create_full_prediction_dataframe("2023-01-01", "2023-01-10", make_split(), make_config())
        assert "ds" in result.columns
        assert len(result) == 10

    @staticmethod
    def test_weekly_freq_produces_correct_row_count():
        result = create_full_prediction_dataframe("2023-01-02", "2023-01-30", make_split(), make_config(freq="W-MON"))
        # Mondays in range: 2, 9, 16, 23, 30 → 5 rows
        assert len(result) == 5

    @staticmethod
    def test_logistic_growth_adds_cap_and_floor_from_train():
        split = make_split(cap=120.0, floor=0.0)
        result = create_full_prediction_dataframe("2023-01-01", "2023-01-05", split, make_config(growth="logistic"))
        assert "cap" in result.columns
        assert "floor" in result.columns
        assert (result["cap"] == 120.0).all()
        assert (result["floor"] == 0.0).all()

    @staticmethod
    def test_logistic_cap_reflects_max_cap_in_train():
        split = make_split(cap=200.0, floor=5.0)
        result = create_full_prediction_dataframe("2023-01-01", "2023-01-03", split, make_config(growth="logistic"))
        assert (result["cap"] == 200.0).all()
        assert (result["floor"] == 5.0).all()

    @staticmethod
    def test_linear_growth_does_not_add_cap_or_floor():
        result = create_full_prediction_dataframe(
            "2023-01-01", "2023-01-05", make_split(), make_config(growth="linear")
        )
        assert "cap" not in result.columns
        assert "floor" not in result.columns

    @staticmethod
    def test_pass_culture_months_adds_boolean_column_marked_true_for_matching_months():
        config = make_config(pass_culture_months=["01-2023"])
        result = create_full_prediction_dataframe("2023-01-01", "2023-01-05", make_split(), config)
        assert "pass_culture_months" in result.columns
        assert result["pass_culture_months"].all()  # all rows are in Jan 2023

    @staticmethod
    def test_pass_culture_months_marks_false_for_non_matching_months():
        config = make_config(pass_culture_months=["03-2023"])
        result = create_full_prediction_dataframe("2023-01-01", "2023-01-05", make_split(), config)
        assert "pass_culture_months" in result.columns
        assert not result["pass_culture_months"].any()  # Jan 2023 is not in the list

    @staticmethod
    def test_no_pass_culture_months_does_not_add_column():
        result = create_full_prediction_dataframe("2023-01-01", "2023-01-05", make_split(), make_config())
        assert "pass_culture_months" not in result.columns
