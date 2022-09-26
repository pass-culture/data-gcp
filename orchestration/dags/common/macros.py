from datetime import datetime, timedelta
from base64 import b32decode
import binascii
import os
from common.config import (
    BIGQUERY_OPEN_DATA_PUBLIC_DATASET,
    BIGQUERY_OPEN_DATA_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_BACKEND_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    BASE32_JS_LIB_PATH,
)


def yyyymmdd(ds):
    if ds is None:
        return datetime.today().strftime("%Y%m%d")
    if isinstance(ds, str):
        ds = datetime.strptime(ds, "%Y-%m-%d")
    return ds.strftime("%Y%m%d")


def today():
    return datetime.today().strftime("%Y-%m-%d")


def current_month(ds):
    if ds is None:
        ds = datetime.today().strftime("%Y%m%d")
    if isinstance(ds, str):
        ds = datetime.strptime(ds, "%Y-%m-%d")
    return ds.replace(day=1).strftime("%Y-%m-%d")


def yesterday():
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def add_days(ds, days):
    if ds is None:
        ds = datetime.now()
    if isinstance(ds, str):
        ds = datetime.strptime(ds, "%Y-%m-%d")
    return ds + timedelta(days=days)


def create_humanize_id_function():
    PATH_TO_DIR = os.path.dirname(os.path.realpath(__file__))
    # Define function humanize_id(int) -> str
    humanize_id_definition_query = f"""
        CREATE TEMPORARY FUNCTION humanize_id(id STRING)
        RETURNS STRING
        LANGUAGE js
        OPTIONS (
            library="{BASE32_JS_LIB_PATH}"
        )
        AS \"\"\"
    """

    # open js file and copy code
    with open(os.path.join(PATH_TO_DIR, "js", "humanize_id.js")) as js_file:
        js_code = "\t\t\t".join(js_file.readlines())
        return f"""{humanize_id_definition_query} \t\t {js_code} \"\"\";"""


class NonDehumanizableId(Exception):
    pass


def dehumanize(public_id: str | None) -> int | None:
    if public_id is None:
        return None
    missing_padding = len(public_id) % 8
    if missing_padding != 0:
        public_id += "=" * (8 - missing_padding)
    try:
        xbytes = b32decode(public_id.replace("8", "O").replace("9", "I"))
    except binascii.Error:
        raise NonDehumanizableId("id non dehumanizable")
    return int_from_bytes(xbytes)


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
    "current_month": current_month,
    "yesterday": yesterday,
    "add_days": add_days,
    "create_humanize_id_function": create_humanize_id_function,
}
