import datetime
import math

import matplotlib.pyplot as plt
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)


def run_prophet_evaluation(
    df,
    start_date,
    end_date,
    target_name,
    datetime_name,
    prophet_params,
    freq,
    cross_val=False,
    train_prop=0.7,
    cv_initial="100 W",
    cv_period="3 W",
    cv_horizon="12 W",
    plot_train=False,
    plot_changepoints=True,
    plot_components=True,
    plot_cv=True,
    verbose=True,
    adding_country_holidays=False,
    add_monthly_seasonality=False,
    pass_culture_months=None,
    monthly_fourier_order=5,
    regressors=None,
    backtest=True,
    cap=None,
    floor=None,
    xlabel="weeks",
):
    """
    Runs Prophet evaluation pipeline: test set or cross-validation.
    Args:
        df: DataFrame with time series data.
        start_date, end_date: Strings for IS/OOS split.
        target_name, datetime_name: Column names.
        prophet_params: Dict of Prophet parameters.
        train_prop: Proportion for train split (ignored if cross_val=True).
        freq: Frequency string (default 'W').
        cross_val: If True, run cross-validation, else test/backtest.
        cv_initial, cv_period, cv_horizon: CV parameters.
        plot_train, plot_changepoints, plot_cv: Plotting options.
        verbose: Print metrics and info.
        adding_country_holidays: If True, add country holidays to the model.
        add_monthly_seasonality: If True, add custom monthly seasonality.
        monthly_fourier_order: Fourier order for monthly seasonality. defaults to 5,
                                higher when more complex seasonality patters
        pass_culture_months: If passed, add custom pass culture seasonality.
                        includes rentrée scolaire, summer breaks, marketing campaigns.
                        Used if and only if regressors includes
                        'pass_culture_seasonal_effect' column. with boolean values
                        indicating presence of seasonal effect on corresponding dates.
        regressors: List of regressor column names (optional).
        cap: Optional cap value for logistic growth.
        floor: Optional floor value for logistic growth.
    Returns:
        Dict with results and metrics.
    """
    # Prepare data
    if cross_val:
        train_prop = 1.0
    # If regressors are specified, keep all columns for split, then filter
    df_train, df_test, df_backtest = prepare_data(
        df,
        start_date,
        end_date,
        target_name,
        datetime_name,
        train_prop=train_prop,
        pass_culture_months=pass_culture_months,
        cap=cap,
        floor=floor,
    )
    print(df_train)
    if regressors:
        cols = ["ds", "y", *regressors]
        df_train = df_train[cols]
        df_test = df_test[cols]
        df_backtest = df_backtest[cols]
    model = initialize_prophet_model(prophet_params)
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
    model = train_prophet_model(df_train, model)
    results = {}
    results["model"] = model
    if verbose:
        print(f"Number of changepoints {len(model.changepoints)}")

    # Add full train forecast
    forecast_train = model.predict(df_train)
    forecast_train["y"] = df_train["y"].values
    results["forecast_train"] = forecast_train

    if plot_changepoints:
        plot_prophet_changepoints(model, df_train)
        plot_trend_with_changepoints(results, model.changepoints)
    if plot_components:
        model.plot_components(model.predict(df_train))

    if cross_val:
        cv_results, cv_metrics = cross_validate_prophet_model(
            model,
            initial=cv_initial,
            period=cv_period,
            horizon=cv_horizon,
            metrics=("mae", "rmse", "mape"),
            plot=plot_cv,
        )
        if verbose:
            print("Cross-validation metrics:", cv_metrics)
        results["cv_results"] = cv_results
        results["cv_metrics"] = cv_metrics
    else:
        test_periods = df_test.shape[0]
        forecast_test, metrics_test = evaluate_prophet_model(
            model,
            df_test,
            df_train,
            test_periods,
            freq=freq,
            plot_train=plot_train,
            is_print=verbose,
        )
        results["forecast_test"] = forecast_test
        results["metrics_test"] = metrics_test

    if backtest:
        forecast_backtest, metrics_backtest = evaluate_prophet_backtest(
            model,
            df_backtest,
            df_test,
            freq=freq,
            is_print=verbose,
            cap=cap,
            floor=floor,
            x_label=xlabel,
        )
        results["forecast_backtest"] = forecast_backtest
        results["metrics_backtest"] = metrics_backtest

    # Add full test and backtest forecasts (already present above, but for clarity)
    # results["forecast_test"] and results["forecast_backtest"] are already set
    return results


def cross_validate_prophet_model(
    model, initial, period, horizon, metrics=("mae", "rmse", "mape"), plot=True
):
    """
    Perform cross-validation for a Prophet model using rolling windows.
    Example:
        initial = 2 years
        period  = 30 days
        horizon = 90 days

        Fold 1:
            Train on first 2 years
            Forecast next 90 days
        Fold 2 (shift by 30 days):
            Train on first 2 years + 30 days
            Forecast next 90 days
        Fold 3 (shift another 30 days):
            Train on first 2 years + 60 days
            Forecast next 90 days
    Args:
        model: Trained Prophet model.
        initial: String, size of the initial training period (e.g., '730 days').
        period: String, spacing between cutoff dates (e.g., '180 days').
        horizon: String, forecast horizon (e.g., '365 days').
        df: Optional, DataFrame used for fitting (required for plotting changepoints).
        metrics: Tuple of metrics to compute ('mae', 'rmse', 'mape').
        plot: Whether to plot cross-validation results.
    Returns:
        cv_results: DataFrame with cross-validation results.
        metrics_dict: Dictionary with mean metrics.
    """

    cv_results = cross_validation(
        model, initial=initial, period=period, horizon=horizon, parallel=None
    )
    perf = performance_metrics(cv_results)
    metrics_dict = {m: perf[m].mean() for m in metrics if m in perf.columns}
    if plot:
        import matplotlib.pyplot as plt

        for m in metrics:
            if m in perf.columns:
                plt.figure(figsize=(8, 4))
                plt.plot(perf["horizon"], perf[m], marker="o", linestyle="-", label=m)
                plt.xlabel("Horizon")
                plt.ylabel(m.upper())
                plt.title(f"Prophet CV: {m.upper()} by Horizon")
                plt.legend()
                plt.tight_layout()
                plt.show()
    return cv_results, metrics_dict


def initialize_prophet_model(prophet_params):
    model = Prophet(**prophet_params)
    return model


def train_prophet_model(df_train, model):
    model.fit(df_train)  # train
    return model


def forecast_prophet_model(model, test_periods, freq="W"):
    future = model.make_future_dataframe(
        periods=test_periods, freq=freq, include_history=False
    )
    forecast = model.predict(future)
    return forecast


def plot_prophet_changepoints(
    model, df_train, title="Prophet Changepoints on Train Set", figsize=(12, 6)
):
    """
    Plots the training set with Prophet changepoints
    and displays the number of changepoints.
    Args:
        model: Trained Prophet model.
        df_train: DataFrame with training data (columns: ds, y).
        title: Plot title.
        figsize: Figure size.
    """

    plt.figure(figsize=figsize)
    plt.plot(df_train.ds, df_train.y, label="train", color="tab:green")
    for cp in model.changepoints:
        plt.axvline(cp, color="red", linestyle="--", alpha=0.7)
    plt.title(f"{title}\nNumber of changepoints: {len(model.changepoints)}")
    plt.xlabel("Date")
    plt.ylabel("y")
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_trend_with_changepoints(results, changepoints):
    plt.figure(figsize=(10, 6))
    plt.plot(results["forecast_train"].ds, results["forecast_train"]["trend"])
    # Annotate changepoints
    # Add vertical lines for changepoints
    for cp in changepoints:
        plt.axvline(
            pd.to_datetime(cp),
            color="orange",
            linestyle="--",
            alpha=0.8,
        )

    for cp in changepoints:
        plt.text(
            pd.to_datetime(cp),
            plt.ylim()[1] * 0.95,
            cp,
            rotation=90,
            color="orange",
            va="top",
            ha="right",
            fontsize=9,
        )
    plt.title("Trend with Changepoints")
    plt.xlabel("Date")
    plt.ylabel("Trend")
    plt.show()


def evaluate_prophet_backtest(
    model,
    df_backtest,
    df_test,
    freq="W",
    title="Backtest Forecast",
    x_label="weeks",
    y_label="Pricing €",
    figsize=(12, 8),
    is_print=True,
    cap=None,
    floor=None,
):
    """
    Evaluates a Prophet model on a backtest set: forecasts, computes metrics,
    and plots results.
    Args:
        model: Trained Prophet model.
        df_backtest: DataFrame with backtest set (columns: ds, y).
        backtest_periods: Number of periods to forecast (if None, uses len(df_backtest))
        freq: Frequency string for Prophet (default 'W').
        title, x_label, y_label, figsize: Plotting options.
        is_print: Whether to print metrics.
    Returns:
        forecast: Prophet forecast DataFrame.
        metrics: dict with MAE, RMSE, MAPE.
    """
    backtest_periods = len(df_backtest)
    test_periods = len(df_test)
    future = model.make_future_dataframe(
        periods=backtest_periods + test_periods,
        freq=freq,
        include_history=False,  # make futures create dataframe since last train date,
        # so add test periods to get the real backtest set
    )
    future = future.tail(backtest_periods)
    if cap is not None:
        future["cap"] = cap
    if floor is not None:
        future["floor"] = floor
    forecast = model.predict(future)
    # Align forecast with backtest set
    forecast_eval = forecast.copy()
    forecast_eval["y"] = df_backtest["y"].values
    MAE = mean_absolute_error(df_backtest["y"], forecast_eval["yhat"])
    RMSE = root_mean_squared_error(df_backtest["y"], forecast_eval["yhat"])
    MAPE = mean_absolute_percentage_error(df_backtest["y"], forecast_eval["yhat"])
    metrics = {"MAE": MAE, "RMSE": RMSE, "MAPE": MAPE}
    if is_print:
        print("Backtest MAE/semaine en euros:", MAE, "€")
        print("Backtest RMSE/semaine en euros:", RMSE, "€")
        print("Backtest MAPE:", 100 * MAPE, "%")
    # Plot
    plt.figure(figsize=figsize)
    plt.plot(df_backtest.ds, df_backtest.y, label="backtest", color="tab:orange")
    plt.plot(
        forecast_eval.ds, forecast_eval.yhat, label="forecast y_hat", color="tab:blue"
    )
    plt.plot(
        forecast_eval.ds,
        forecast_eval.yhat_upper,
        label="y_hat upper",
        color="slategrey",
    )
    plt.plot(
        forecast_eval.ds,
        forecast_eval.yhat_lower,
        label="y_hat lower",
        color="slategrey",
    )
    plt.fill_between(
        forecast_eval.ds,
        forecast_eval.yhat_upper,
        forecast_eval.yhat_lower,
        color="lightblue",
    )
    plt.legend()
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.show()
    return forecast_eval, metrics


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
    return df_IS[:train_cuttoff], df_IS[train_cuttoff:], df_OOS


def return_metrics(df_test, forecast, is_print=True):
    MAE = mean_absolute_error(df_test["y"], forecast["yhat"])
    RMSE = root_mean_squared_error(df_test["y"], forecast["yhat"])
    MAPE = mean_absolute_percentage_error(df_test["y"], forecast["yhat"])
    if is_print:
        print("MAE/semaine en euros:", MAE, "€")
        print("RMSE/semaine en euros:", RMSE, "€")
        print("MAPE:", 100 * MAPE, "%")


def evaluate_prophet_model(
    model,
    df_test,
    df_train=None,
    test_periods=None,
    freq="W",
    title="Forecast",
    x_label="weeks",
    y_label="Pricing €",
    figsize=(12, 8),
    plot_train=False,
    is_print=True,
):
    """
    Evaluates a Prophet model: forecasts, computes metrics, and plots results.
    Args:
        model: Trained Prophet model.
        df_test: DataFrame with test set (columns: ds, y).
        df_train: DataFrame with train set (optional, columns: ds, y).
        test_periods: Number of periods to forecast (if None, uses len(df_test)).
        freq: Frequency string for Prophet (default 'W').
        title, x_label, y_label, figsize, plot_train: Plotting options.
        is_print: Whether to print metrics.
    Returns:
        forecast: Prophet forecast DataFrame.
        metrics: dict with MAE, RMSE, MAPE.
    """
    if test_periods is None:
        test_periods = len(df_test)
    future = model.make_future_dataframe(
        periods=test_periods, freq=freq, include_history=False
    )
    forecast = model.predict(future)
    # Align forecast with test set
    forecast_eval = forecast.copy()
    forecast_eval["y"] = df_test["y"].values
    MAE = mean_absolute_error(df_test["y"], forecast_eval["yhat"])
    RMSE = root_mean_squared_error(df_test["y"], forecast_eval["yhat"])
    MAPE = mean_absolute_percentage_error(df_test["y"], forecast_eval["yhat"])
    metrics = {"MAE": MAE, "RMSE": RMSE, "MAPE": MAPE}
    if is_print:
        print("MAE/semaine en euros:", MAE, "€")
        print("RMSE/semaine en euros:", RMSE, "€")
        print("MAPE:", 100 * MAPE, "%")
    # Plot
    plt.figure(figsize=figsize)
    if plot_train and df_train is not None:
        plt.plot(df_train.ds, df_train.y, label="train", color="tab:green")
    plt.plot(df_test.ds, df_test.y, label="test", color="tab:orange")
    plt.plot(
        forecast_eval.ds, forecast_eval.yhat, label="forecast y_hat", color="tab:blue"
    )
    plt.plot(
        forecast_eval.ds,
        forecast_eval.yhat_upper,
        label="y_hat upper",
        color="slategrey",
    )
    plt.plot(
        forecast_eval.ds,
        forecast_eval.yhat_lower,
        label="y_hat lower",
        color="slategrey",
    )
    plt.fill_between(
        forecast_eval.ds,
        forecast_eval.yhat_upper,
        forecast_eval.yhat_lower,
        color="lightblue",
    )
    plt.legend()
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.show()
    return forecast_eval, metrics


def create_full_prediction_dataframe(
    start_date, end_date, freq="W-MON", cap=None, floor=None
):
    df = pd.DataFrame({"ds": pd.date_range(start=start_date, end=end_date, freq=freq)})
    if cap is not None:
        df["cap"] = cap
    if floor is not None:
        df["floor"] = floor
    return df
