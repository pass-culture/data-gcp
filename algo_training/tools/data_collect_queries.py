import pandas as pd
from utils import ENV_SHORT_NAME,GCP_PROJECT_ID

def get_data(data_type):
    query = f"""
        select * from `{GCP_PROJECT_ID}.raw_{ENV_SHORT_NAME}.training_data_{data_type}`
    """
    data = pd.read_gbq(query)
    return data
