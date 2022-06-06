from datetime import datetime

from dependencies.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)


def yyyymmdd():
    return datetime.today().strftime("%Y%m%d")

def today():
    return datetime.today().strftime("%Y-%m-%d")


default = {
    "bigquery_analytics_dataset": BIGQUERY_ANALYTICS_DATASET,
    "bigquery_clean_dataset": BIGQUERY_CLEAN_DATASET,
    "bigquery_raw_dataset": BIGQUERY_RAW_DATASET,
    "env_short_name": ENV_SHORT_NAME,
    "gcp_project": GCP_PROJECT,
    "yyyymmdd": yyyymmdd,
    "today": today
}
