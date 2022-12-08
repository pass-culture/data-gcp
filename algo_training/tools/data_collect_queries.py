import pandas as pd
from utils import ENV_SHORT_NAME, GCP_PROJECT_ID


def get_data(dataset: str, data_type: str):
    query = f"""
        select * from `{GCP_PROJECT_ID}.{dataset}.training_data_{data_type}`
    """
    data = pd.read_gbq(query)
    return data
