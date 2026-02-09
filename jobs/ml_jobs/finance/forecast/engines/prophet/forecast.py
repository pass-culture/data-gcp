import pandas as pd
from loguru import logger

from forecast.engines.prophet.model_config import ModelConfig
from forecast.forecasters.forecast_model import DataSplit
from prophet import Prophet


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
    df = pd.DataFrame(
        {
            "ds": pd.date_range(
                start=start_date, end=end_date, freq=model_config.evaluation.freq
            )
        }
    )
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
