import datetime
import math

from prophet import Prophet


def prepare_data(
    df,
    start_date,
    end_date,
    target_name,
    datetime_name,
    train_prop=0.7,
    pass_culture_months=None,
    cap=None,
    floor=None,
    regressors=None,
):
    """
    Docstring for prepare_data

    :param df: dataframe containing time series data
    :param start_date: in sample start date
    :param end_date: oos start date
    :param target_name: the y column
    :param datetime_name: the column that contain datetimme aka ds in prophet
    :param train_prop: proportion of train in the in sample dataset
    :param pass_culture_months: a list of months where we want to enforce pass culture
    related seasonality events
    e.g. ["07-2023", "09-2023"] for marketing campaigns, rentrée scolaire, etc
    """
    df = df.rename(columns={datetime_name: "ds", target_name: "y"})
    if pass_culture_months:
        # Support months as 'MM-YYYY' strings
        months_years = set(pass_culture_months)
        df["_month_year"] = df["ds"].dt.strftime("%m-%Y")
        df["pass_culture_seasonal_effect"] = df["_month_year"].isin(months_years)
        df.drop(columns=["_month_year"], inplace=True)

    if cap is not None:
        df["cap"] = cap
    if floor is not None:
        df["floor"] = floor

    df_IS = df[
        (df["ds"] >= datetime.datetime.fromisoformat(start_date))
        & (df["ds"] <= datetime.datetime.fromisoformat(end_date))
    ]
    df_OOS = df[df["ds"] > datetime.datetime.fromisoformat(end_date)]
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
    prophet_params,
    df_train,
    adding_country_holidays=False,
    add_monthly_seasonality=False,
    monthly_fourier_order=5,
    pass_culture_months=None,
    regressors=None,
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
            period=30.5,
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
