import pandas as pd


def _create_full_prediction_dataframe(
    start_date, end_date, freq="W-MON", cap=None, floor=None
):
    """
    Create a prophet compatible dataframe for predictions:
    Given start_date and end_date, this function return a simple dataframe
    with one column 'ds'
    :param start_date: start date of the prediction period, format 'YYYY-MM-DD'
    :param end_date: end date of the prediction period, format 'YYYY-MM-DD'
    :param freq: frequency string for date range generation, e.g., 'W-MON'
                (week starting on Monday)
    :param cap: optional cap value for logistic growth prophet models
    :param floor: optional floor value for logistic growth prophet models
    """

    df = pd.DataFrame({"ds": pd.date_range(start=start_date, end=end_date, freq=freq)})
    if cap is not None:
        df["cap"] = cap
    if floor is not None:
        df["floor"] = floor
    return df


def predict_prophet_model(model, start_date, end_date, freq="W", cap=None, floor=None):
    """
    Generate forecasts using a trained Prophet model for a specified date range.
    Args:
        model: Trained Prophet model.
        start_date: Start date for predictions (string 'YYYY-MM-DD').
        end_date: End date for predictions (string 'YYYY-MM-DD').
        freq: Frequency string for date range generation (default 'W').
        cap: Optional cap value for logistic growth models.
        floor: Optional floor value for logistic growth models.
    Returns:
        forecast: DataFrame with forecasts.
    """
    df_future = _create_full_prediction_dataframe(
        start_date, end_date, freq, cap, floor
    )
    forecast = model.predict(df_future)
    return forecast
