import pandas as pd
from utils import ENV_SHORT_NAME, GCP_PROJECT_ID


def get_data(dataset: str, table_name: str):
    query = f"""
        select * from `{GCP_PROJECT_ID}.{dataset}.{table_name}`
    """
    data = pd.read_gbq(query)
    return data
