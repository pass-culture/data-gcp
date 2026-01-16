import pandas as pd
from loguru import logger
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)

from src.prophet.model_config import DataProcConfig, EvaluationConfig
from src.prophet.plots import plot_cv_results, plot_forecast_vs_actuals


def cross_validate(
    model: Prophet,
    initial: str,
    period: str,
    horizon: str,
    metrics: tuple[str, ...] = ("mae", "rmse", "mape"),
) -> tuple[pd.DataFrame, dict[str, float]]:
    """
    Perform cross-validation for a Prophet model using rolling windows.

    Cross-validation evaluates model performance by training on progressively
    larger datasets and forecasting fixed horizons.

    Example:
        initial = '730 days' (2 years)
        period  = '30 days'
        horizon = '90 days'

        Fold 1: Train on first 2 years → Forecast next 90 days
        Fold 2: Train on first 2 years + 30 days → Forecast next 90 days
        Fold 3: Train on first 2 years + 60 days → Forecast next 90 days

    Args:
        model: Trained Prophet model.
        initial: Size of the initial training period (e.g., '730 days').
        period: Spacing between cutoff dates (e.g., '180 days').
        horizon: Forecast horizon (e.g., '365 days').
        metrics: Tuple of metrics to compute ('mae', 'rmse', 'mape').

    Returns:
        perf: DataFrame with cross-validation performance metrics for each fold.
        metrics_dict: Dictionary with mean metrics across all folds
    """
    logger.info(
        f"Starting cross-validation: initial={initial}, period={period}, "
        f"horizon={horizon}"
    )
    cv_results = cross_validation(
        model, initial=initial, period=period, horizon=horizon, parallel=None
    )
    perf = performance_metrics(cv_results)

    metrics_dict = {m: perf[m].mean() for m in metrics if m in perf.columns}
    logger.info(f"Cross-validation complete. Mean metrics: {metrics_dict}")

    return perf, metrics_dict


def forecast_and_return_truth(model: Prophet, df_actual: pd.DataFrame) -> pd.DataFrame:
    """
    Generate forecasts using the Prophet model and return a DataFrame
    that includes both forecasts and actual values.
    Args:
        model: Trained Prophet model.
        df_actual: DataFrame containing actual values with 'ds' and 'y' columns.
    Returns:
        forecast: Prophet forecast DataFrame merged with actuals.
                  Contains 'ds', 'yhat', 'y', and Prophet output columns.
    """

    # Prepare columns for prediction
    predict_cols = ["ds"]
    if "cap" in df_actual.columns and "floor" in df_actual.columns:
        predict_cols += ["cap", "floor"]

    # Generate forecast and merge with actuals
    forecast = model.predict(df_actual[predict_cols])
    forecast = forecast.merge(df_actual[["ds", "y"]], on="ds", how="left")
    return forecast


def compute_metrics(forecast: pd.DataFrame) -> dict[str, float]:
    """
    Compute evaluation metrics between actuals and forecasts.

    Args:
        forecast: DataFrame containing 'yhat' and 'y' columns.
    Returns:
        Dictionary with MAE, RMSE, and MAPE metrics.
    """

    # Compute error metrics
    mae = mean_absolute_error(forecast["y"], forecast["yhat"])
    rmse = root_mean_squared_error(forecast["y"], forecast["yhat"])
    mape = mean_absolute_percentage_error(forecast["y"], forecast["yhat"])

    metrics = {"MAE": mae, "RMSE": rmse, "MAPE": mape}
    logger.info(f"Evaluation metrics: MAE={mae:.4f}, RMSE={rmse:.4f}, MAPE={mape:.4f}")

    return metrics


def evaluation_pipeline(
    model: Prophet,
    df_test: pd.DataFrame,
    df_backtest: pd.DataFrame,
    data_proc_config: DataProcConfig,
    evaluation_config: EvaluationConfig,
    *,
    run_backtest: bool,
) -> dict:
    """
    Evaluate the Prophet model and return results for logging.

    Performs either cross-validation or standard test set evaluation based on
    configuration. Optionally evaluates on a backtest (out-of-sample) dataset.

    Args:
        model: Trained Prophet model.
        df_test: DataFrame for test evaluation (used when CV is disabled).
        df_backtest: DataFrame for backtest evaluation (out-of-sample).
        data_proc_config: Data processing configuration with CV flag.
        evaluation_config: Evaluation configuration with CV parameters and frequency.
        run_backtest: Whether to evaluate on backtest data.

    Returns:
        Dictionary containing evaluation outputs:
            - 'cv': Results when CV is enabled:
                    {'perf': DataFrame, 'metrics': dict, 'figs': list}
            - 'test': Results when CV is disabled:
                      {'metrics': dict, 'forecast_df': DataFrame, 'forecast_path': str}
            - 'backtest': Results when run_backtest is True:
                          {'metrics': dict, 'forecast_df': DataFrame,
                           'forecast_path': str, 'fig': Figure}
    """
    results: dict = {}

    # Perform cross-validation or test set evaluation
    if data_proc_config.cv:
        logger.info("Performing cross-validation evaluation")
        cv_perf, cv_metrics = cross_validate(
            model,
            evaluation_config.cv_initial,
            evaluation_config.cv_period,
            evaluation_config.cv_horizon,
            metrics=("mae", "rmse", "mape"),
        )
        # Plot CV results (pass list of metric names)
        cv_figs = plot_cv_results(cv_perf, list(cv_metrics.keys()))
        results["cv"] = {"metrics": cv_metrics, "figs": cv_figs}
    else:
        logger.info("Performing test set evaluation")
        test_forecast_df = forecast_and_return_truth(model, df_test)
        test_metrics = compute_metrics(test_forecast_df)
        results["test"] = {
            "metrics": test_metrics,
        }

    # Perform backtest evaluation if requested
    if run_backtest:
        logger.info("Performing backtest evaluation")
        backtest_forecast_df = forecast_and_return_truth(model, df_backtest)
        backtest_metrics = compute_metrics(backtest_forecast_df)

        # Plot forecast vs actuals using correct frequency from config
        fig = plot_forecast_vs_actuals(
            forecast=backtest_forecast_df,
            freq=evaluation_config.freq,
            title="Backtest Forecast vs Actuals",
        )
        results["backtest"] = {
            "metrics": backtest_metrics,
            "fig": fig,
        }

    return results
