import datetime
import json
import math

import pandas as pd
from loguru import logger
from prophet import Prophet

from src.utils.bigquery import load_table


def load_data_and_params(
    *, table_name: str, model_name: str, cv: bool, train_prop: float
) -> tuple[pd.DataFrame, dict, float]:
    """
    Docstring for load_data_and_params

    Args:
        table_name: str
        model_name: str
        cv: bool
        train_prop: float

    returns:
        tuple[pd.DataFrame, dict, float]
    """
    df = load_table(table_name)
    if cv:
        train_prop = 1.0
        logger.info("CV enabled: using all data for training")
    with open(f"prophet_params/{model_name}.json") as f:
        train_params = json.load(f)
    return df, train_params, train_prop


def clean_and_prepare_df(
    df: pd.DataFrame,
    date_column_name: str,
    target_name: str,
    OOS_end_date: str,
    pass_culture_months: list[str],
    growth: str,
) -> pd.DataFrame:
    """
    Cleans and prepares the dataframe for Prophet modeling.

    - Converts date column to datetime.
    - Renames columns for Prophet compatibility.
    - Filters data before OOS_end_date.
    - Adds pass_culture_seasonal_effect if needed.
    - Adds floor/cap columns for logistic growth.

    Args:
        df: Input dataframe.
        date_column_name: Name of the date column.
        target_name: Name of the target column.
        OOS_end_date: Out-of-sample end date (exclusive).
        pass_culture_months: List of 'MM-YYYY' strings for special seasonality.
        growth: Prophet growth type ("linear" or "logistic").

    Returns:
        pd.DataFrame: Cleaned dataframe.
    """
    df[date_column_name] = pd.to_datetime(df[date_column_name])
    df = df.rename(columns={date_column_name: "ds", target_name: "y"})
    df = df[df["ds"] < OOS_end_date].reset_index(drop=True)

    if pass_culture_months:
        months_years = set(pass_culture_months)
        df["_month_year"] = df["ds"].dt.strftime("%m-%Y")
        df["pass_culture_seasonal_effect"] = df["_month_year"].isin(months_years)
        df.drop(columns=["_month_year"], inplace=True)
    if growth == "logistic":
        df["floor"] = 0.0
        df["cap"] = df["y"].max() * 1.2
    return df


def split_train_test_backtest(
    df: pd.DataFrame,
    IS_start_date: str,
    OOS_start_date: str,
    train_prop: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Splits the dataframe into train, test, and backtest sets.

    Args:
        df: Cleaned dataframe with 'ds' and 'y' columns.
        IS_start_date: In-sample start date (inclusive).
        OOS_start_date: Out-of-sample start date (inclusive).
        train_prop: Proportion of train in the in-sample dataset.
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:(df_train, df_test, df_backtest)
    """
    df_IS = df[
        (df["ds"] >= datetime.datetime.fromisoformat(IS_start_date))
        & (df["ds"] <= datetime.datetime.fromisoformat(OOS_start_date))
    ]
    df_OOS = df[df["ds"] > datetime.datetime.fromisoformat(OOS_start_date)]
    train_cuttoff = math.ceil(df_IS.shape[0] * train_prop)
    df_train = df_IS.iloc[:train_cuttoff].copy().reset_index(drop=True)
    df_test = df_IS.iloc[train_cuttoff:].copy().reset_index(drop=True)
    df_backtest = df_OOS.copy().reset_index(drop=True)
    return df_train, df_test, df_backtest


def select_regressor_columns(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    df_backtest: pd.DataFrame,
    regressors: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Keeps only the relevant columns for Prophet modeling.

    Args:
        df_train: Training dataframe.
        df_test: Test dataframe.
        df_backtest: Backtest dataframe.
        regressors: List of regressor column names.
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:(df_train, df_test, df_backtest)
        with selected columns.
    """
    if regressors:
        cols = ["ds", "y", *regressors]
        df_train = df_train[cols]
        df_test = df_test[cols]
        df_backtest = df_backtest[cols]
    return df_train, df_test, df_backtest


def prepare_data(
    *,
    df: pd.DataFrame,
    IS_start_date: str,
    OOS_start_date: str,
    OOS_end_date: str,
    target_name: str,
    date_column_name: str,
    train_prop: float,
    pass_culture_months: list[str],
    growth: str,
    regressors: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepares the data for Prophet model training and evaluation.

    Args:
        df: Dataframe containing time series data.
        IS_start_date: In-sample start date.
        OOS_start_date: Out-of-sample start date.
        OOS_end_date: Out-of-sample end date.
        target_name: The y column.
        date_column_name: The column that contains datetime (aka ds in Prophet).
        train_prop: Proportion of train in the in-sample dataset.
        pass_culture_months: List of months for special seasonality events.
        growth: Prophet growth type ("linear" or "logistic").
        regressors: List of regressor column names.
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:(df_train, df_test, df_backtest)
    """
    df_clean = clean_and_prepare_df(
        df,
        date_column_name,
        target_name,
        OOS_end_date,
        pass_culture_months,
        growth,
    )
    df_train, df_test, df_backtest = split_train_test_backtest(
        df_clean, IS_start_date, OOS_start_date, train_prop
    )
    df_train, df_test, df_backtest = select_regressor_columns(
        df_train, df_test, df_backtest, regressors
    )
    return df_train, df_test, df_backtest


def fit_prophet_model(
    df_train: pd.DataFrame,
    prophet_params: dict,
    *,
    regressors: list[str],
    adding_country_holidays: bool,
    add_monthly_seasonality: bool,
    monthly_fourier_order: int,
    pass_culture_months: list[str],
) -> Prophet:
    """Fit a Prophet model with specified parameters and additional features.
    Args:
        df_train: Training dataframe with 'ds' and 'y' columns.
        prophet_params: Dictionary of Prophet parameters.
        regressors: List of regressor column names to add to the model.
        adding_country_holidays: Boolean, whether to add country holidays.
        add_monthly_seasonality: Boolean, whether to add custom monthly seasonality.
        monthly_fourier_order: Fourier order for the monthly seasonality.
        pass_culture_months: List of months for special seasonality events.
    Returns:
        Trained Prophet model.
    """
    model = Prophet(**prophet_params)
    if regressors:
        for reg in regressors:
            model.add_regressor(reg)
    if adding_country_holidays:
        model.add_country_holidays(country_name="FR")
    if add_monthly_seasonality:
        model.add_seasonality(
            name="monthly",
            period=30.5,  # monthly seasonality
            fourier_order=monthly_fourier_order,
        )
    if pass_culture_months:
        model.add_seasonality(
            name="pass_culture",
            period=30.5,  # monthly seasonality
            fourier_order=3,
            condition_name="pass_culture_seasonal_effect",
        )
    model.fit(df_train)
    return model
