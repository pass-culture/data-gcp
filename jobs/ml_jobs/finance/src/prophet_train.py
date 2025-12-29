import datetime
import math

import pandas as pd
from prophet import Prophet


def prepare_data(
    *,
    df,
    IS_start_date,
    OOS_start_date,
    OOS_end_date,
    target_name,
    date_column_name,
    train_prop,
    pass_culture_months,
    growth,
    regressors,
):
    """
    Prepares the data for Prophet model training and evaluation.
    If cross_val is True, df_test will be empty in that case.

    :param df: dataframe containing time series data
    :param start_date: in sample start date
    :param end_date: oos start date
    :param target_name: the y column
    :param date_column_name: the column that contains datetime aka ds in prophet
    :param train_prop: proportion of train in the in sample dataset
    :param pass_culture_months: a list of months where we want to enforce pass culture
    related seasonality events
    e.g. ["07-2023", "09-2023"] for marketing campaigns, rentrée scolaire, etc
    """
    ## Clean and prepare data
    df[date_column_name] = pd.to_datetime(df[date_column_name])
    df = df.rename(columns={date_column_name: "ds", target_name: "y"})

    ## Limit to data before OOS end date in case data is not fully available
    df = df[df["ds"] < OOS_end_date].reset_index(drop=True)

    if pass_culture_months:
        # Support months as 'MM-YYYY' strings
        months_years = set(pass_culture_months)
        df["_month_year"] = df["ds"].dt.strftime("%m-%Y")
        df["pass_culture_seasonal_effect"] = df["_month_year"].isin(months_years)
        df.drop(columns=["_month_year"], inplace=True)
    if growth == "logistic":
        df["floor"] = 0.0
        df["cap"] = df["y"].max() * 1.2

    df_IS = df[
        (df["ds"] >= datetime.datetime.fromisoformat(IS_start_date))
        & (df["ds"] <= datetime.datetime.fromisoformat(OOS_start_date))
    ]
    df_OOS = df[df["ds"] > datetime.datetime.fromisoformat(OOS_start_date)]
    train_cuttoff = math.ceil(df_IS.shape[0] * train_prop)
    df_train = df_IS.iloc[:train_cuttoff].copy().reset_index(drop=True)
    df_test = df_IS.iloc[train_cuttoff:].copy().reset_index(drop=True)
    df_backtest = df_OOS.copy().reset_index(drop=True)

    if regressors:
        cols = ["ds", "y", *regressors]
        df_train = df_train[cols]
        df_test = df_test[cols]
        df_backtest = df_backtest[cols]
    return df_train, df_test, df_backtest


def fit_prophet_model(
    df_train,
    prophet_params,
    *,
    regressors,
    adding_country_holidays,
    add_monthly_seasonality,
    monthly_fourier_order,
    pass_culture_months,
):
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
