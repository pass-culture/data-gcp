import matplotlib.pyplot as plt
from prophet.diagnostics import cross_validation, performance_metrics
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)


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
