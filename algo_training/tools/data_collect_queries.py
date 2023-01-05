import json

import pandas as pd
from utils import GCP_PROJECT_ID


def get_data(
    dataset: str,
    table_name: str,
    max_limit: int = None,
    subcategory_ids: str = None,
    event_day_number: str = None,
):
    query_filter = ""
    limit_filter = ""
    if subcategory_ids:
        # Convert list to tuple to use BigQuery's list format
        subcategory_ids = tuple(json.loads(subcategory_ids))
        query_filter += f"WHERE offer_subcategoryid in {subcategory_ids}"
    if event_day_number:
        # Filter the event date by the last 'event_day_number' days
        query_filter += "WHERE " if len(query_filter) == 0 else " AND "
        query_filter += (
            f"event_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -{event_day_number} DAY) "
        )
    if max_limit:
        limit_filter = f"LIMIT {max_limit}"
    query = f"""
        SELECT * FROM `{GCP_PROJECT_ID}.{dataset}.{table_name}` {query_filter} {limit_filter}
    """
    data = pd.read_gbq(query)
    return data


def get_column_data(
    dataset: str,
    table_name: str,
    column_name: str,
):
    query = f"""
        SELECT DISTINCT {column_name} FROM `{GCP_PROJECT_ID}.{dataset}.{table_name}`
    """
    data = pd.read_gbq(query)
    return data
