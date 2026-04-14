import pandas as pd
from loguru import logger
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)

from forecast.engines.prophet.model_config import ModelConfig
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics


def _convert_cv_param(param: float, training_period: int, freq: str) -> str:
    """
    Convert CV parameter to time string for Prophet.

    Args:
        param: a float percentage like 0.6
        training_period: Total number of periods in the training data (days or weeks)
        freq: Frequency string ('D' for daily, 'W-MON' for weekly, etc.)

    Returns:
        String in Prophet's format (e.g., "730 days" or "100 W")
    """

    periods = int(param * training_period)

    # Determine unit based on frequency
    if freq.startswith("W"):
        return f"{periods} W"
    return f"{periods} days"


def cross_validate(
    model: Prophet,
    model_config: ModelConfig,
) -> dict[str, float]:
    """
    Perform cross-validation for a Prophet model using rolling windows.

    Cross-validation evaluates model performance by training on progressively
    larger datasets and forecasting fixed horizons.
    initial: Size of the initial training period (e.g., '730 days').
    period: Spacing between cutoff dates (e.g., '180 days').
    horizon: Forecast horizon (e.g., '365 days').

    Example:
        initial = '730 days' (2 years)
        period  = '30 days'
        horizon = '90 days'

        Fold 1: Train on first 2 years → Forecast next 90 days
        Fold 2: Train on first 2 years + 30 days → Forecast next 90 days
        Fold 3: Train on first 2 years + 60 days → Forecast next 90 days

    Args:
        model: Trained Prophet model.
        model_config: Model configuration containing CV parameters.

    Returns:
        perf: DataFrame with cross-validation performance metrics for each fold.
        metrics_dict: Dictionary with mean metrics across all folds
                        (e.g., {'MAE': 123.45, 'RMSE': 234.56, 'MAPE': 0.12}).
    """
    metrics = ["MAE", "RMSE", "MAPE"]

    # Calculate training data length from model history
    freq = model_config.evaluation.freq
    training_period = len(model.history)

    # Convert CV params (handle both string and percentage formats)
    initial = _convert_cv_param(model_config.evaluation.cv_initial, training_period, freq)
    period = _convert_cv_param(model_config.evaluation.cv_period, training_period, freq)
    horizon = _convert_cv_param(model_config.evaluation.cv_horizon, training_period, freq)

    logger.info(f"Starting cross-validation: initial={initial}, period={period}, horizon={horizon}")
    cv_results = cross_validation(model, initial=initial, period=period, horizon=horizon, parallel=None)
    perf = performance_metrics(cv_results)

    if perf is not None:
        metrics_dict = {m: perf[m].mean() for m in metrics if m in perf.columns}
        logger.info(f"Cross-validation complete. Mean metrics: {metrics_dict}")
    else:
        metrics_dict = {}
        logger.warning("Cross-validation failed to produce performance metrics.")

    return metrics_dict


def predict_with_truth(model: Prophet, df_actual: pd.DataFrame) -> pd.DataFrame:
    """
    Generate forecasts using the Prophet model and return a DataFrame
    that includes both forecasts and true values.
    Args:
        model: Trained Prophet model.
        df_actual: DataFrame containing true values in the 'y' column and date in 'ds'.
    Returns:
        forecast: Prophet forecast DataFrame merged with actuals.
                  Contains 'ds', 'yhat', 'y', and Prophet output columns.
    """
    # Prophet discards "y" column during prediction
    # so it is safe to pass the df_actual directly here
    # Ensure ds is datetime
    if "ds" in df_actual.columns:
        df_actual["ds"] = pd.to_datetime(df_actual["ds"])

    forecast = model.predict(df_actual)
    if "ds" in forecast.columns:
        forecast["ds"] = pd.to_datetime(forecast["ds"])

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
    model_config: ModelConfig,
) -> dict:
    """
    Evaluate the Prophet model and return results for logging.

    Performs either cross-validation or standard test set evaluation based on
    configuration.

    Args:
        model: Trained Prophet model.
        df_test: DataFrame for test evaluation (used when CV is disabled).
        model_config: Model configuration.

    Returns:
        Dictionary containing evaluation outputs: {'MAE': ..., 'RMSE': ..., 'MAPE': ...}

    """

    # Perform cross-validation or test set evaluation
    if model_config.evaluation.cv:
        logger.info("Performing cross-validation evaluation")
        metrics = cross_validate(model, model_config)
    else:
        logger.info("Performing test set evaluation")
        test_forecast_df = predict_with_truth(model, df_test)
        metrics = compute_metrics(test_forecast_df)

    return metrics


def _aggregate_forecast_monthly(forecast: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate forecast/actual values at monthly level.
    """
    df = forecast.copy()
    df["ds"] = pd.to_datetime(df["ds"])

    agg_map = {col: "sum" for col in ["y", "yhat", "yhat_lower", "yhat_upper"] if col in df.columns}

    monthly = df.set_index("ds").resample("MS").agg(agg_map).reset_index().sort_values("ds")
    return monthly


def backtest_pipeline(df_backtest: pd.DataFrame, model: Prophet) -> tuple[dict, pd.DataFrame]:
    """Perform backtest evaluation on monthly aggregated predictions.
    Args:
        df_backtest: DataFrame for backtest evaluation.
        model: Trained Prophet model.
    Returns:
        Tuple containing:
            - Dictionary with backtest evaluation metrics (monthly level)
            - DataFrame with backtest forecast at original frequency (ds, y, yhat, etc)
    """
    logger.info("Performing backtest evaluation (monthly aggregated metrics)")
    backtest_forecast_df = predict_with_truth(model, df_backtest)

    # compute monthly aggregated metrics for backtest evaluation
    backtest_forecast_monthly_df = _aggregate_forecast_monthly(backtest_forecast_df)
    backtest_metrics = compute_metrics(backtest_forecast_monthly_df)

    return backtest_metrics, backtest_forecast_df
