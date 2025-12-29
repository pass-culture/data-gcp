import pandas as pd


def create_full_prediction_dataframe(start_date, end_date, freq, cap, floor):
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


def predict_prophet_model(model, df_predict):
    """
    Generate forecasts using a trained Prophet model for a specified date range.
    Args:
        model: Trained Prophet model.
        df_predict: DataFrame with 'ds' column for prediction dates.
                    If model uses logistic growth, should also include 'cap' and 'floor'
                    columns.
    Returns:
        forecast: DataFrame with forecasts.
    """
    forecast = model.predict(df_predict)
    return forecast
