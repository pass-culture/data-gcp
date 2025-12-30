import pandas as pd
from prophet import Prophet


def create_full_prediction_dataframe(
    start_date: str, end_date: str, freq: str, cap=None, floor=None
) -> pd.DataFrame:
    """
    Create a prophet compatible dataframe for predictions:
    Given start_date and end_date, this function return a simple dataframe
    with one column 'ds'
    Args:
    start_date: start date of the prediction period, format 'YYYY-MM-DD'
    end_date: end date of the prediction period, format 'YYYY-MM-DD'
    freq: frequency string for date range generation, e.g., 'W-MON'
                (week starting on Monday)
    cap: optional cap value for logistic growth prophet models
    floor: optional floor value for logistic growth prophet models
    Returns:
    DataFrame with 'ds' column and optional 'cap' and 'floor' columns.
    """

    df = pd.DataFrame({"ds": pd.date_range(start=start_date, end=end_date, freq=freq)})
    if cap is not None:
        df["cap"] = cap
    if floor is not None:
        df["floor"] = floor
    return df


def predict_prophet_model(model: Prophet, df_predict: pd.DataFrame) -> pd.DataFrame:
    """
    Generate forecasts using a trained Prophet model for a specified date range.
    Args:
        model: Trained Prophet model.
        df_predict: DataFrame with 'ds' column for prediction dates.
                    If model uses logistic growth, should also include 'cap' and 'floor'
                    columns.
    Returns:
        DataFrame with forecasts.
    """
    return model.predict(df_predict)
