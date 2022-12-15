import json

import pandas as pd
from utils import ENV_SHORT_NAME, GCP_PROJECT_ID


def get_data(dataset: str, table_name: str, subcategory_ids: str = None):
    query_filter = ""
    if subcategory_ids:
        # Convert list to tuple to use BigQuery's list format
        subcategory_ids = tuple(json.loads(subcategory_ids))
        query_filter += f"WHERE offer_subcategoryid in {subcategory_ids}"
    query = f"""
        select * from `{GCP_PROJECT_ID}.{dataset}.{table_name}` {query_filter}
    """
    data = pd.read_gbq(query)
    return data