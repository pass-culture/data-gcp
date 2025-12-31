import mlflow
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)

from src.prophet.plots import (
    plot_cv_results,
    plot_forecast_vs_actuals,
)
from src.prophet.predict import predict_prophet_model


def cross_validate_prophet_model(
    model: Prophet,
    initial: str,
    period: str,
    horizon: str,
    metrics: tuple = ("mae", "rmse", "mape"),
) -> tuple[pd.DataFrame, dict]:
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
        metrics: tuple of metrics to compute ('mae', 'rmse', 'mape').
    Returns:
        perf: DataFrame with performance metrics by horizon.
        metrics_dict: Dictionary with mean metrics.
    """

    cv_results = cross_validation(
        model, initial=initial, period=period, horizon=horizon, parallel=None
    )
    perf = performance_metrics(cv_results)

    metrics_dict = {m: perf[m].mean() for m in metrics if m in perf.columns}
    return perf, metrics_dict


def evaluate_prophet_model(
    model: Prophet, df_actual: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """
    Evaluate a Prophet model over a specified date range.
    Args:
        model: Trained Prophet model.
        df_actual: DataFrame with actual values (columns: 'ds', 'y').
    Returns:
        forecast: Prophet forecast DataFrame. contains columns 'ds', 'yhat', 'y',
                and all the other columns from prophet output
        metrics: dict with MAE, RMSE, MAPE.
    """
    predict_cols = ["ds"]
    if "cap" in df_actual.columns and "floor" in df_actual.columns:
        predict_cols += ["cap", "floor"]

    forecast = predict_prophet_model(model, df_actual[predict_cols])
    # merge forecast with actuals
    forecast = forecast.merge(df_actual[["ds", "y"]], on="ds", how="left")

    ## compute metrics
    mae = mean_absolute_error(forecast["y"], forecast["yhat"])
    rmse = root_mean_squared_error(forecast["y"], forecast["yhat"])
    mape = mean_absolute_percentage_error(forecast["y"], forecast["yhat"])

    metrics = {"MAE": mae, "RMSE": rmse, "MAPE": mape}

    return forecast, metrics


def evaluate_and_log(
    *,
    model: Prophet,
    df_test: pd.DataFrame,
    df_backtest: pd.DataFrame,
    cv: bool,
    cv_initial: str,
    cv_period: str,
    cv_horizon: str,
    backtest: bool,
) -> None:
    """Evaluate the Prophet model and log results to MLflow.
    Args:
    model: Trained Prophet model.
    df_test: DataFrame for test evaluation.
    df_backtest: DataFrame for backtest evaluation.
    cv: Boolean, whether to perform cross-validation.
    cv_initial: String, initial training period for CV.
    cv_period: String, period between cutoffs for CV.
    cv_horizon: String, forecast horizon for CV.
    backtest: Boolean, whether to evaluate on backtest data.

    Returns: None
    """
    if cv:
        cv_metrics, cv_perf = cross_validate_prophet_model(
            model,
            cv_initial,
            cv_period,
            cv_horizon,
            metrics=("mae", "rmse", "mape"),
        )
        for k, v in cv_metrics.items():
            mlflow.log_metric(f"cv_{k}", v)
        cv_figs = plot_cv_results(cv_perf, cv_metrics)
        for metric, fig in cv_figs.items():
            mlflow.log_figure(fig, f"plots/cv_{metric}.png")
    else:
        test_forecast_df, test_metrics = evaluate_prophet_model(model, df_test)
        for k, v in test_metrics.items():
            mlflow.log_metric(f"test_{k}", v)
        test_xlsx = "test_forecast.xlsx"
        test_forecast_df.to_excel(test_xlsx, index=False)
        mlflow.log_artifact(test_xlsx, artifact_path="data")
    if backtest:
        backtest_forecast_df, backtest_metrics = evaluate_prophet_model(
            model, df_backtest
        )
        for k, v in backtest_metrics.items():
            mlflow.log_metric(f"backtest_{k}", v)
        backtest_xlsx = "backtest_forecast.xlsx"
        backtest_forecast_df.to_excel(backtest_xlsx, index=False)
        mlflow.log_artifact(backtest_xlsx, artifact_path="data")
        fig = plot_forecast_vs_actuals(
            forecast=backtest_forecast_df,
            freq="D",
            title="Backtest Forecast vs Actuals",
        )
        mlflow.log_figure(fig, "plots/forecast_vs_actuals_backtest.png")
