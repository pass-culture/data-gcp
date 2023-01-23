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
    # If no columns are specified we select all columns
    columns_selected = columns_selected or ["*"]

    # String containing all query filters
    filters = ""
    if event_day_number:
        filters += f"WHERE event_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -{event_day_number} DAY) "
    if max_limit:
        filters += f" LIMIT {max_limit}"

    data = pd.read_gbq(
        f"""
            SELECT {", ".join(columns_selected)} FROM `{GCP_PROJECT_ID}.{dataset}.{table_name}` {filters}
        """
    )
    return data
