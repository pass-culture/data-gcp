import os
from datetime import datetime, timedelta

from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    BASE32_JS_LIB_PATH,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_BACKEND_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_INT_API_GOUV_DATASET,
    BIGQUERY_INT_APPLICATIVE_DATASET,
    BIGQUERY_INT_FIREBASE_DATASET,
    BIGQUERY_INT_GEOLOCATION_DATASET,
    BIGQUERY_INT_RAW_DATASET,
    BIGQUERY_ML_FEATURES_DATASET,
    BIGQUERY_ML_PREPROCESSING_DATASET,
    BIGQUERY_ML_RECOMMENDATION_DATASET,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_SANDBOX_DATASET,
    BIGQUERY_SEED_DATASET,
    BIGQUERY_TMP_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
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


def last_week():
    return (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")


def add_days(ds, days):
    if ds is None:
        ds = datetime.now()
    if isinstance(ds, str):
        ds = datetime.strptime(ds, "%Y-%m-%d")
    return (ds + timedelta(days=days)).strftime("%Y-%m-%d")


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
    "bigquery_int_firebase_dataset": BIGQUERY_INT_FIREBASE_DATASET,
    "bigquery_int_applicative_dataset": BIGQUERY_INT_APPLICATIVE_DATASET,
    "bigquery_int_geo_dataset": BIGQUERY_INT_GEOLOCATION_DATASET,
    "bigquery_tmp_dataset": BIGQUERY_TMP_DATASET,
    "bigquery_clean_dataset": BIGQUERY_CLEAN_DATASET,
    "bigquery_raw_dataset": BIGQUERY_RAW_DATASET,
    "bigquery_int_raw_dataset": BIGQUERY_INT_RAW_DATASET,
    "bigquery_seed_dataset": BIGQUERY_SEED_DATASET,
    "bigquery_int_api_gouv_dataset": BIGQUERY_INT_API_GOUV_DATASET,
    "bigquery_sandbox_dataset": BIGQUERY_SANDBOX_DATASET,
    "bigquery_backend_dataset": BIGQUERY_BACKEND_DATASET,
    "bigquery_export_dataset": f"{GCP_PROJECT_ID}.export_{ENV_SHORT_NAME}",
    "bigquery_ml_reco_dataset": BIGQUERY_ML_RECOMMENDATION_DATASET,
    "bigquery_ml_feat_dataset": BIGQUERY_ML_FEATURES_DATASET,
    "bigquery_ml_preproc_dataset": BIGQUERY_ML_PREPROCESSING_DATASET,
    "bigquery_appsflyer_import_dataset": f"{GCP_PROJECT_ID}.appsflyer_import_{ENV_SHORT_NAME}",
    "bigquery_int_contentful_dataset": f"{GCP_PROJECT_ID}.int_contentful_{ENV_SHORT_NAME}",
    "env_short_name": ENV_SHORT_NAME,
    "gcp_project_id": GCP_PROJECT_ID,
    "yyyymmdd": yyyymmdd,
    "today": today,
    "current_month": current_month,
    "yesterday": yesterday,
    "last_week": last_week,
    "add_days": add_days,
    "create_humanize_id_function": create_humanize_id_function,
    "create_dehumanize_id_function": create_dehumanize_id_function,
    "mediation_url": MEDIATION_URL,
    "applicative_external_connection_id": APPLICATIVE_EXTERNAL_CONNECTION_ID,
}
