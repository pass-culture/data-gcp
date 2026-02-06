import datetime
import os
import re

import pandas as pd

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")

today = datetime.datetime.now()
days3 = datetime.timedelta(days=3)
days7 = datetime.timedelta(days=7)
month = datetime.timedelta(weeks=4)
year = datetime.timedelta(days=365)


def get_schedule_mapping():
    """
    Get the schedule mapping for determining table update thresholds.

    Returns a dictionary mapping schedule types to datetime thresholds.
    Tables that haven't been updated since their respective threshold
    will be flagged as potentially stale.

    Returns:
        dict: A mapping of schedule types to datetime objects representing
              the threshold date for each schedule:
              - 'daily': 3 days ago
              - 'weekly': 7 days ago
              - 'monthly': 4 weeks ago
              - 'yearly': 365 days ago
              - 'default': 4 weeks ago (fallback for untagged tables)
              - 'never': Never alert (epoch time)
    """
    return {
        "daily": today - days3,
        "weekly": today - days7,
        "monthly": today - month,
        "yearly": today - year,
        "default": today - month,
        "never": datetime.datetime(1970, 1, 1),
    }


def get_datasets_to_scan():
    query = f"""
    SELECT
        distinct schema_name
        FROM {GCP_PROJECT}.`region-europe-west1`.INFORMATION_SCHEMA.SCHEMATA;
    """
    df = pd.read_gbq(query)

    datasets = df["schema_name"].tolist()

    datasets_to_scan = [
        dataset
        for dataset in datasets
        if dataset.startswith("int_")
        or dataset.startswith("ml_")
        or dataset
        in [
            f"analytics_{ENV_SHORT_NAME}",
            f"clean_{ENV_SHORT_NAME}",
            f"raw_{ENV_SHORT_NAME}",
            f"raw_applicative_{ENV_SHORT_NAME}",
            f"snapshot_{ENV_SHORT_NAME}",
            f"backend_{ENV_SHORT_NAME}",
        ]
    ]

    return datasets_to_scan


def table_name_contains_partition_date(table_name):
    pattern = r"(\d{4})(\d{2})(\d{2})"

    matches = re.findall(pattern, table_name)

    for match in matches:
        year, month, day = match
        try:
            datetime.datetime(int(year), int(month), int(day))
            return True
        except ValueError:
            continue

    return False


def get_last_update_date(datasets_to_scan):
    table_last_update_list = []
    for dataset in datasets_to_scan:
        last_update_query = f"""
            SELECT
                table_id as table_name,
                dataset_id as table_schema,
                TIMESTAMP_MILLIS(last_modified_time) as last_modified_time
            FROM {dataset}.__TABLES__
        """
        table_last_update_list.append(pd.read_gbq(last_update_query))

    table_last_update_df = pd.concat(table_last_update_list, axis=0).reset_index(
        drop=True
    )

    return table_last_update_df


def get_table_schedule():
    schedule_query = f"""
            SELECT
            table_schema,
            table_name,
            REGEXP_EXTRACT(option_value, r'"schedule",\s*"([^"]+)"') as schedule_tag
            FROM
            `{GCP_PROJECT}`.`region-europe-west1`.INFORMATION_SCHEMA.TABLE_OPTIONS
            WHERE option_name = 'labels';
        """

    table_schedule_df = pd.read_gbq(schedule_query)

    return table_schedule_df
