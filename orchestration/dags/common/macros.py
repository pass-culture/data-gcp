from datetime import datetime, timedelta
import os
from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_BACKEND_DATASET,
    BIGQUERY_TMP_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    BASE32_JS_LIB_PATH,
    BIGQUERY_SANDBOX_DATASET,
    MEDIATION_URL,
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


def create_js_function(name, filename):
    PATH_TO_DIR = os.path.dirname(os.path.realpath(__file__))
    # Define function humanize_id(int) -> str
    query = f"""
        CREATE TEMPORARY FUNCTION {name}(id STRING)
        RETURNS STRING
        LANGUAGE js
        OPTIONS (
            library="{BASE32_JS_LIB_PATH}"
        )
        AS \"\"\"
    """

    # open js file and copy code
    with open(os.path.join(PATH_TO_DIR, "js", filename)) as js_file:
        js_code = "\t\t\t".join(js_file.readlines())
        return f"""{query} \t\t {js_code} \"\"\";"""


def create_humanize_id_function():
    return create_js_function("humanize_id", "humanize_id.js")


def create_dehumanize_id_function():
    return create_js_function("dehumanize_id", "dehumanize_id.js")


default = {
    "bigquery_analytics_dataset": BIGQUERY_ANALYTICS_DATASET,
    "bigquery_tmp_dataset": BIGQUERY_TMP_DATASET,
    "bigquery_clean_dataset": BIGQUERY_CLEAN_DATASET,
    "bigquery_raw_dataset": BIGQUERY_RAW_DATASET,
    "bigquery_sandbox_dataset": BIGQUERY_SANDBOX_DATASET,
    "bigquery_backend_dataset": BIGQUERY_BACKEND_DATASET,
    "env_short_name": ENV_SHORT_NAME,
    "gcp_project": GCP_PROJECT_ID,
    "yyyymmdd": yyyymmdd,
    "today": today,
    "current_month": current_month,
    "yesterday": yesterday,
    "add_days": add_days,
    "create_humanize_id_function": create_humanize_id_function,
    "create_dehumanize_id_function": create_dehumanize_id_function,
    "mediation_url": MEDIATION_URL,
}
