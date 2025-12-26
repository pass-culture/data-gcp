import matplotlib.pyplot as plt
from prophet.diagnostics import cross_validation, performance_metrics
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)

from src.prophet_predict import predict_prophet_model


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
        perf: DataFrame with performance metrics by horizon.
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
    return cv_results, perf, metrics_dict


def evaluate_prophet_model(
    model, df_actual, eval_start_date, eval_end_date, freq="W", cap=None, floor=None
):
    """
    Evaluate a Prophet model over a specified date range.
    Args:
        model: Trained Prophet model.
        eval_start_date: Start date for evaluation (string 'YYYY-MM-DD').
        eval_end_date: End date for evaluation (string 'YYYY-MM-DD').
        freq: Frequency string for Prophet (default 'W').
        cap: Optional cap value for logistic growth models.
        floor: Optional floor value for logistic growth models.
    Returns:
        forecast: Prophet forecast DataFrame. contains columns 'ds', 'yhat', 'y',
                and all the other columns from prophet output
        metrics: dict with MAE, RMSE, MAPE.
    """
    forecast = predict_prophet_model(
        model, eval_start_date, eval_end_date, freq, cap, floor
    )
    # merge forecast with actuals
    forecast = forecast.merge(df_actual[["ds", "y"]], on="ds", how="left")

    ## compute metrics
    MAE = mean_absolute_error(forecast["y"], forecast["yhat"])
    RMSE = root_mean_squared_error(forecast["y"], forecast["yhat"])
    MAPE = mean_absolute_percentage_error(forecast["y"], forecast["yhat"])

    metrics = {"MAE": MAE, "RMSE": RMSE, "MAPE": MAPE}

    return forecast, metrics


def plot_forecast_vs_actuals(
    forecast,
    title="Forecast vs Actuals",
    freq="W",
    y_label="Pricing €",
    figsize=(12, 8),
):
    """
    Plots Prophet forecast against actual values.
    Args:
        forecast: DataFrame with Prophet forecasts
                (columns: ds, yhat, yhat_lower, yhat_upper, y).
        title, x_label, y_label, figsize: Plotting options.
    """
    if freq == "W":
        x_label = "weeks"
    elif freq == "D":
        x_label = "days"
    else:
        x_label = "date"

    plt.figure(figsize=figsize)
    plt.plot(forecast.ds, forecast.y, label="actuals", color="tab:orange")
    plt.plot(forecast.ds, forecast.yhat, label="forecast y_hat", color="tab:blue")
    plt.plot(
        forecast.ds,
        forecast.yhat_upper,
        label="y_hat upper",
        color="slategrey",
    )
    plt.plot(
        forecast.ds,
        forecast.yhat_lower,
        label="y_hat lower",
        color="slategrey",
    )
    plt.fill_between(
        forecast.ds,
        forecast.yhat_upper,
        forecast.yhat_lower,
        color="lightblue",
    )
    plt.legend()
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.show()
