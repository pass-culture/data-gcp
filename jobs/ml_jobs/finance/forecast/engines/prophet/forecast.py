import pandas as pd
from loguru import logger

from forecast.engines.prophet.model_config import ModelConfig
from forecast.forecasters.forecast_model import DataSplit
from prophet import Prophet


def attach_future_regressors(
    df: pd.DataFrame,
    model_config: ModelConfig,
    train_test_backtest_split: DataSplit,
) -> pd.DataFrame:
    """Populate regressor columns on the future prediction dataframe.

    Prophet requires a value for every configured regressor on every row being
    predicted, including future dates. Known-deposit roll-off is deterministic,
    but new-cohort future grant volume is not, so the value is filled using a
    configurable strategy (``features.regressor_future_strategy``) rather than
    a forecast of its own.

    Args:
        df: Prediction dataframe already containing 'ds'.
        model_config: Full configuration object holding feature settings.
        train_test_backtest_split: Object containing train/test/backtest splits,
            used as the source of historical regressor values.

    Returns:
        pd.DataFrame: ``df`` with one column added per configured regressor.
    """
    history = pd.concat(
        [train_test_backtest_split.train, train_test_backtest_split.backtest],
        ignore_index=True,
    ).sort_values("ds")

    strategy = model_config.features.regressor_future_strategy
    for reg in model_config.features.regressors or []:
        if reg not in history.columns:
            raise ValueError(f"Regressor '{reg}' is configured but missing from historical data")

        value = history[reg].tail(90).mean() if strategy == "flat_mean_90d" else history[reg].iloc[-1]
        df[reg] = value

    return df


def create_full_prediction_dataframe(
    start_date: str,
    end_date: str,
    train_test_backtest_split: DataSplit,
    model_config: ModelConfig,
) -> pd.DataFrame:
    """
    Create a Prophet-compatible dataframe for predictions.

    Generates a date range from start_date to end_date with specified frequency,
    optionally including cap and floor for logistic growth models.

    Args:
        start_date: Start date of prediction period (YYYY-MM-DD).
        end_date: End date of prediction period (YYYY-MM-DD).
        train_test_backtest_split: Object containing train/test/backtest splits.
        model_config: Full configuration object holding feature and growth settings.

    Returns:
        DataFrame with 'ds' column and optional 'cap' and 'floor' columns.
    """
    logger.info(f"Creating prediction dataframe from {start_date} to {end_date}")
    # Extract cap and floor from prophet config if provided
    cap = None
    floor = None
    if model_config.prophet.growth == "logistic":
        # When using logistic growth, read cap and floor values from df_train
        cap = train_test_backtest_split.train["cap"].max()
        floor = train_test_backtest_split.train["floor"].min()

    df = pd.DataFrame({"ds": pd.date_range(start=start_date, end=end_date, freq=model_config.evaluation.freq)})

    if model_config.features.pass_culture_seasonality:
        df["pass_culture_months"] = pd.to_datetime(df["ds"]).dt.month.isin([6, 12])

    if model_config.features.regressors:
        df = attach_future_regressors(df, model_config, train_test_backtest_split)

    if cap is not None:
        df["cap"] = cap
    if floor is not None:
        df["floor"] = floor

    logger.info(f"Created prediction dataframe with {len(df)} periods")
    return df


def generate_future_forecast(
    model: Prophet,
    train_test_backtest_split: DataSplit,
    forecast_start_date: str,
    forecast_end_date: str,
    model_config: ModelConfig,
) -> pd.DataFrame:
    """Generate a complete future forecast from start_date to end_date.

    Convenience function that creates a prediction dataframe and generates
    the forecast in one call.

    Args:
        model: Trained Prophet model.
        train_test_backtest_split: Object containing train/test/backtest splits.
        forecast_start_date: Start date of forecast period (YYYY-MM-DD).
        forecast_end_date: End date of forecast period (YYYY-MM-DD).
        model_config: Full configuration object with evaluation and growth settings.

    Returns:
        DataFrame with complete forecast including columns like 'ds', 'yhat',
        'yhat_lower', 'yhat_upper', and trend/seasonality components.
    """

    # Create prediction dataframe
    df_future = create_full_prediction_dataframe(
        start_date=forecast_start_date,
        end_date=forecast_end_date,
        train_test_backtest_split=train_test_backtest_split,
        model_config=model_config,
    )

    # Generate forecast
    forecast = model.predict(df_future)

    return forecast
