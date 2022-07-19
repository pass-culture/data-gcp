from datetime import datetime, timedelta

from common.config import (
    BIGQUERY_OPEN_DATA_PUBLIC_DATASET,
    BIGQUERY_OPEN_DATA_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_BACKEND_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
)


def yyyymmdd(ds):
    if ds is None:
        return datetime.today().strftime("%Y%m%d")
    if isinstance(ds, str):
        ds = datetime.strptime(ds, "%Y-%m-%d")
    return ds.strftime("%Y%m%d")


def today():
    return datetime.today().strftime("%Y-%m-%d")


def yesterday():
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def add_days(ds, days):
    if ds is None:
        ds = datetime.now()
    if isinstance(ds, str):
        ds = datetime.strptime(ds, "%Y-%m-%d")
    return ds + timedelta(days=days)


default = {
    "bigquery_open_data_project": BIGQUERY_OPEN_DATA_PROJECT,
    "bigquery_open_data_public_dataset": BIGQUERY_OPEN_DATA_PUBLIC_DATASET,
    "bigquery_analytics_dataset": BIGQUERY_ANALYTICS_DATASET,
    "bigquery_clean_dataset": BIGQUERY_CLEAN_DATASET,
    "bigquery_raw_dataset": BIGQUERY_RAW_DATASET,
    "bigquery_backend_dataset": BIGQUERY_BACKEND_DATASET,
    "env_short_name": ENV_SHORT_NAME,
    "gcp_project": GCP_PROJECT,
    "yyyymmdd": yyyymmdd,
    "today": today,
    "yesterday": yesterday,
    "add_days": add_days,
}
