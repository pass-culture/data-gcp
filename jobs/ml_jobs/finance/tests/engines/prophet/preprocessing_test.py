"""Tests for forecast.engines.prophet.preprocessing — CRITIQUE & ÉLEVÉ gaps.

Pure functions only: no external boundaries to mock.
"""

import math

import pandas as pd
import pytest

from forecast.engines.prophet.model_config import ModelConfig
from forecast.engines.prophet.preprocessing import (
    add_features,
    filter_data,
    select_feature_columns,
    split_train_test_backtest,
    validate_data,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_config(
    growth: str = "linear",
    train_prop: float = 0.8,
    cv: bool = False,
    date_col: str = "date",
    target_col: str = "revenue",
    pass_culture_months=None,
    regressors=None,
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
                "regressors": regressors,
            },
            "data_processing": {
                "train_prop": train_prop,
                "table_name": "test_table",
                "date_column_name": date_col,
                "target_name": target_col,
            },
            "evaluation": {
                "cv": cv,
                "cv_initial": 0.7,
                "cv_period": 0.15,
                "cv_horizon": 0.1,
                "freq": "D",
            },
        }
    )


# ---------------------------------------------------------------------------
# validate_data  (🟠 Moyen — tested here for completeness of the module)
# ---------------------------------------------------------------------------


class TestValidateData:
    @staticmethod
    def test_raises_on_empty_dataframe():
        config = make_config()
        with pytest.raises(ValueError, match="empty"):
            validate_data(pd.DataFrame(), config)

    @staticmethod
    def test_raises_when_required_column_missing():
        config = make_config(date_col="date", target_col="revenue")
        df = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=3)})  # missing "revenue"
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_data(df, config)

    @staticmethod
    def test_passes_when_all_required_columns_present():
        config = make_config(date_col="date", target_col="revenue")
        df = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=3), "revenue": [1.0, 2.0, 3.0]})
        validate_data(df, config)  # must not raise

    @staticmethod
    def test_raises_when_regressor_column_missing():
        config = make_config(regressors=["extra_feature"])
        df = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=3), "revenue": [1.0, 2.0, 3.0]})
        with pytest.raises(ValueError, match="extra_feature"):
            validate_data(df, config)


# ---------------------------------------------------------------------------
# filter_data  🚨 CRITIQUE
# ---------------------------------------------------------------------------


class TestFilterData:
    @staticmethod
    def test_renames_columns_to_prophet_format():
        df = pd.DataFrame(
            {
                "date": pd.date_range("2020-01-01", periods=10, freq="D"),
                "revenue": range(10),
            }
        )
        result = filter_data(df, backtest_end_date="2020-01-15", model_config=make_config())
        assert "ds" in result.columns
        assert "y" in result.columns
        assert "date" not in result.columns
        assert "revenue" not in result.columns

    @staticmethod
    def test_filters_rows_strictly_before_backtest_end_date():
        df = pd.DataFrame(
            {
                "date": pd.date_range("2020-01-01", periods=10, freq="D"),
                "revenue": range(10),
            }
        )
        result = filter_data(df, backtest_end_date="2020-01-06", model_config=make_config())
        # 2020-01-01 … 2020-01-05 (strictly before 2020-01-06) → 5 rows
        assert len(result) == 5
        assert pd.to_datetime(result["ds"]).max() < pd.Timestamp("2020-01-06")

    @staticmethod
    def test_backtest_end_date_itself_is_excluded():
        df = pd.DataFrame(
            {
                "date": [pd.Timestamp("2020-06-01")],
                "revenue": [42.0],
            }
        )
        result = filter_data(df, backtest_end_date="2020-06-01", model_config=make_config())
        assert result.empty

    @staticmethod
    def test_returns_empty_when_all_rows_after_end():
        df = pd.DataFrame(
            {
                "date": pd.date_range("2021-01-01", periods=5, freq="D"),
                "revenue": range(5),
            }
        )
        result = filter_data(df, backtest_end_date="2020-01-01", model_config=make_config())
        assert result.empty

    @staticmethod
    def test_resets_index():
        df = pd.DataFrame(
            {
                "date": pd.date_range("2020-01-01", periods=5, freq="D"),
                "revenue": range(5),
            },
            index=[10, 20, 30, 40, 50],
        )
        result = filter_data(df, backtest_end_date="2021-01-01", model_config=make_config())
        assert list(result.index) == list(range(len(result)))


# ---------------------------------------------------------------------------
# add_features  🚨 CRITIQUE
# ---------------------------------------------------------------------------


class TestAddFeatures:
    @staticmethod
    def test_logistic_growth_adds_floor_and_cap():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=5, freq="D"),
                "y": [10.0, 20.0, 30.0, 40.0, 50.0],
            }
        )
        result = add_features(df, model_config=make_config(growth="logistic"))
        assert "floor" in result.columns
        assert "cap" in result.columns
        assert (result["floor"] == 0.0).all()
        assert (result["cap"] == 50.0 * 1.2).all()

    @staticmethod
    def test_linear_growth_does_not_add_floor_or_cap():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=5, freq="D"),
                "y": [10.0, 20.0, 30.0, 40.0, 50.0],
            }
        )
        result = add_features(df, model_config=make_config(growth="linear"))
        assert "floor" not in result.columns
        assert "cap" not in result.columns

    @staticmethod
    def test_cap_equals_1_point_2_times_max_y():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=3, freq="D"),
                "y": [5.0, 100.0, 50.0],
            }
        )
        result = add_features(df, model_config=make_config(growth="logistic"))
        assert (result["cap"] == 100.0 * 1.2).all()

    @staticmethod
    def test_pass_culture_months_adds_boolean_column():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=6, freq="MS"),
                "y": range(6),
            }
        )
        config = make_config(pass_culture_months=["01-2020", "03-2020"])
        result = add_features(df, model_config=config)
        assert "pass_culture_months" in result.columns
        jan_flag = result.loc[result["ds"] == pd.Timestamp("2020-01-01"), "pass_culture_months"].values[0]
        feb_flag = result.loc[result["ds"] == pd.Timestamp("2020-02-01"), "pass_culture_months"].values[0]
        mar_flag = result.loc[result["ds"] == pd.Timestamp("2020-03-01"), "pass_culture_months"].values[0]
        assert bool(jan_flag) is True
        assert bool(feb_flag) is False
        assert bool(mar_flag) is True

    @staticmethod
    def test_no_pass_culture_months_does_not_add_column():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=3, freq="D"),
                "y": [1.0, 2.0, 3.0],
            }
        )
        result = add_features(df, model_config=make_config())
        assert "pass_culture_months" not in result.columns


# ---------------------------------------------------------------------------
# select_feature_columns  (🟠 Moyen — tested here for completeness)
# ---------------------------------------------------------------------------


class TestSelectFeatureColumns:
    @staticmethod
    def test_base_columns_always_kept():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=3),
                "y": [1.0, 2.0, 3.0],
                "extra": [9, 9, 9],
            }
        )
        result = select_feature_columns(df, model_config=make_config())
        assert list(result.columns) == ["ds", "y"]

    @staticmethod
    def test_regressor_column_included_when_configured():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=3),
                "y": [1.0, 2.0, 3.0],
                "promo": [0, 1, 0],
            }
        )
        config = make_config(regressors=["promo"])
        result = select_feature_columns(df, model_config=config)
        assert "promo" in result.columns

    @staticmethod
    def test_logistic_growth_includes_cap_and_floor():
        df = pd.DataFrame(
            {
                "ds": pd.date_range("2020-01-01", periods=3),
                "y": [1.0, 2.0, 3.0],
                "cap": [1.2, 1.2, 1.2],
                "floor": [0.0, 0.0, 0.0],
            }
        )
        result = select_feature_columns(df, model_config=make_config(growth="logistic"))
        assert "cap" in result.columns
        assert "floor" in result.columns


# ---------------------------------------------------------------------------
# split_train_test_backtest  🚨 CRITIQUE
# ---------------------------------------------------------------------------


class TestSplitTrainTestBacktest:
    @staticmethod
    def _make_df(start="2020-01-01", end="2022-12-31"):
        dates = pd.date_range(start, end, freq="D")
        return pd.DataFrame({"ds": dates, "y": range(len(dates))})

    @staticmethod
    def test_non_cv_split_respects_train_proportion():
        df = TestSplitTrainTestBacktest._make_df()
        config = make_config(train_prop=0.8, cv=False)
        result = split_train_test_backtest(
            df,
            train_start_date="2020-01-01",
            backtest_start_date="2021-01-01",
            model_config=config,
        )
        in_sample = df[(df["ds"] >= pd.Timestamp("2020-01-01")) & (df["ds"] <= pd.Timestamp("2021-01-01"))]
        expected_train_size = math.ceil(len(in_sample) * 0.8)
        assert len(result.train) == expected_train_size

    @staticmethod
    def test_cv_mode_uses_all_in_sample_data_for_train():
        df = TestSplitTrainTestBacktest._make_df()
        config = make_config(cv=True)
        result = split_train_test_backtest(
            df,
            train_start_date="2020-01-01",
            backtest_start_date="2021-01-01",
            model_config=config,
        )
        # With CV, train_prop overridden to 1.0 → test set must be empty
        assert result.test.empty

    @staticmethod
    def test_cv_mode_train_contains_all_in_sample_rows():
        df = TestSplitTrainTestBacktest._make_df()
        config = make_config(cv=True)
        result = split_train_test_backtest(
            df,
            train_start_date="2020-01-01",
            backtest_start_date="2021-01-01",
            model_config=config,
        )
        in_sample = df[(df["ds"] >= pd.Timestamp("2020-01-01")) & (df["ds"] <= pd.Timestamp("2021-01-01"))]
        assert len(result.train) == len(in_sample)

    @staticmethod
    def test_backtest_contains_rows_strictly_after_backtest_start():
        df = TestSplitTrainTestBacktest._make_df()
        result = split_train_test_backtest(
            df,
            train_start_date="2020-01-01",
            backtest_start_date="2021-01-01",
            model_config=make_config(),
        )
        assert not result.backtest.empty
        assert (result.backtest["ds"] > pd.Timestamp("2021-01-01")).all()

    @staticmethod
    def test_no_overlap_between_splits():
        df = TestSplitTrainTestBacktest._make_df()
        result = split_train_test_backtest(
            df,
            train_start_date="2020-01-01",
            backtest_start_date="2021-01-01",
            model_config=make_config(),
        )
        train_dates = set(result.train["ds"])
        test_dates = set(result.test["ds"])
        backtest_dates = set(result.backtest["ds"])
        assert not (train_dates & test_dates), "train and test must not overlap"
        assert not (train_dates & backtest_dates), "train and backtest must not overlap"
        assert not (test_dates & backtest_dates), "test and backtest must not overlap"
