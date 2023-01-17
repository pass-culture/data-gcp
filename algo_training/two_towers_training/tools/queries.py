import json

import pandas as pd

from tools.constants import GCP_PROJECT_ID


def get_data(
    dataset: str,
    table_name: str,
    columns_selected: list = None,
    max_limit: int = None,
    event_day_number: str = None,
):
    query_filter = ""
    limit_filter = ""
    if event_day_number:
        # Filter the event date by the last 'event_day_number' days
        query_filter += "WHERE " if len(query_filter) == 0 else " AND "
        query_filter += (
            f"event_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -{event_day_number} DAY) "
        )
    if max_limit:
        limit_filter = f"LIMIT {max_limit}"
    query = f"""
        SELECT {", ".join(columns_selected)} FROM `{GCP_PROJECT_ID}.{dataset}.{table_name}` {query_filter} {limit_filter}
    """
    data = pd.read_gbq(query)
    return data
